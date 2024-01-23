/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.accio.main.wireprotocol;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.net.HostAndPort;
import io.accio.base.sql.SqlConverter;
import io.accio.cache.CacheManager;
import io.accio.cache.CachedTableMapping;
import io.accio.main.AccioConfig;
import io.accio.main.AccioMetastore;
import io.accio.main.PostgresWireProtocolConfig;
import io.accio.main.metadata.Metadata;
import io.accio.main.netty.ChannelBootstrapFactory;
import io.accio.main.pgcatalog.regtype.RegObjectFactory;
import io.accio.main.wireprotocol.auth.Authentication;
import io.accio.main.wireprotocol.ssl.SslContextProvider;
import io.accio.main.wireprotocol.ssl.SslReqHandler;
import io.airlift.log.Logger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.netty4.Netty4OpenChannelsHandler;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.network.NetworkService.GLOBAL_NETWORK_HOST_SETTING;

public class PostgresNetty
{
    private static final Logger LOGGER = Logger.get(PostgresNetty.class);

    // copied from elasticsearch 7
    public static final Setting<List<String>> GLOBAL_NETWORK_BIND_HOST_SETTING =
            Setting.listSetting("network.bind_host", GLOBAL_NETWORK_HOST_SETTING, Function.identity(), Setting.Property.NodeScope);
    public static final Setting<List<String>> GLOBAL_NETWORK_PUBLISH_HOST_SETTING =
            Setting.listSetting("network.publish_host", GLOBAL_NETWORK_HOST_SETTING, Function.identity(), Setting.Property.NodeScope);

    @Nullable
    private Netty4OpenChannelsHandler openChannels;
    private ServerBootstrap bootstrap;
    private final Settings settings;
    private final SslContextProvider sslContextProvider;
    private final NetworkService networkService;
    private final List<Channel> serverChannels = new ArrayList<>();
    private final List<TransportAddress> boundAddresses = new ArrayList<>();
    private final String port;
    private final int threadCount;
    private final String[] bindHosts;
    private final String[] publishHosts;
    private final RegObjectFactory regObjectFactory;
    private final Metadata connector;

    private final SqlConverter sqlConverter;
    private BoundTransportAddress boundAddress;
    private final AccioMetastore accioMetastore;
    private final CacheManager cacheManager;
    private final CachedTableMapping cachedTableMapping;
    private final AccioConfig accioConfig;
    private final Authentication authentication;

    public PostgresNetty(
            NetworkService networkService,
            PostgresWireProtocolConfig postgresWireProtocolConfig,
            AccioConfig accioConfig,
            SslContextProvider sslContextProvider,
            RegObjectFactory regObjectFactory,
            Metadata connector,
            SqlConverter sqlConverter,
            AccioMetastore accioMetastore,
            CacheManager cacheManager,
            CachedTableMapping cachedTableMapping,
            Authentication authentication)
    {
        this.settings = toWireProtocolSettings();
        this.port = postgresWireProtocolConfig.getPort();
        this.threadCount = postgresWireProtocolConfig.getNettyThreadCount();
        bindHosts = GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings).toArray(new String[0]);
        publishHosts = GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings).toArray(new String[0]);
        this.networkService = networkService;
        this.accioConfig = accioConfig;
        this.sslContextProvider = requireNonNull(sslContextProvider, "sslContextProvider is null");
        this.regObjectFactory = requireNonNull(regObjectFactory, "regObjectFactory is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
        this.accioMetastore = requireNonNull(accioMetastore, "accioMetastore is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.cachedTableMapping = requireNonNull(cachedTableMapping, "cachedTableMapping is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
    }

    public void start()
    {
        this.openChannels = new Netty4OpenChannelsHandler(LOGGER);
        this.bootstrap = ChannelBootstrapFactory.newChannelBootstrap(settings, new NioEventLoopGroup(threadCount));

        bootstrap.childHandler(new ChannelInitializer()
        {
            @Override
            protected void initChannel(Channel ch)
            {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("open_channels", openChannels);
                WireProtocolSession wireProtocolSession =
                        new WireProtocolSession(regObjectFactory, connector, sqlConverter, accioConfig, accioMetastore, cacheManager, cachedTableMapping, authentication);
                PostgresWireProtocol postgresWireProtocol = new PostgresWireProtocol(wireProtocolSession, new SslReqHandler(sslContextProvider));
                pipeline.addLast("frame-decoder", postgresWireProtocol.decoder);
                pipeline.addLast("handler", postgresWireProtocol.handler);
            }
        });

        bootstrap.validate();

        boolean success = false;
        try {
            boundAddress = resolveBindAddress();
            LOGGER.info("Postgre wire protocol server start. Bound Address: %s", boundAddress);
            success = true;
        }
        finally {
            if (!success) {
                // stop
                LOGGER.error("Postgre wire protocol server failed");
                close();
            }
        }
    }

    public HostAndPort getHostAndPort()
    {
        TransportAddress transportAddress = boundAddress.publishAddress();
        return HostAndPort.fromParts(transportAddress.getAddress(), transportAddress.getPort());
    }

    private BoundTransportAddress resolveBindAddress()
    {
        // Bind and start to accept incoming connections.
        try {
            InetAddress[] hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
            for (InetAddress address : hostAddresses) {
                if (address instanceof Inet4Address || address instanceof Inet6Address) {
                    boundAddresses.add(bindAddress(address));
                }
            }
        }
        catch (IOException e) {
            throw new BindPostgresException("Failed to resolve binding network host", e);
        }
        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        }
        catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        final int publishPort = resolvePublishPort(boundAddresses, publishInetAddress);
        final InetSocketAddress publishAddress = new InetSocketAddress(publishInetAddress, publishPort);
        return new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), new TransportAddress(publishAddress));
    }

    private TransportAddress bindAddress(final InetAddress hostAddress)
    {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(portNumber -> {
            try {
                Channel channel = bootstrap.bind(new InetSocketAddress(hostAddress, portNumber)).sync().channel();
                serverChannels.add(channel);
                boundSocket.set((InetSocketAddress) channel.localAddress());
            }
            catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
        });
        if (!success) {
            throw new BindPostgresException("Failed to bind to [" + port + "]", lastException.get());
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Bound psql to address %s", NetworkAddress.format(boundSocket.get()));
        }
        return new TransportAddress(boundSocket.get());
    }

    static int resolvePublishPort(List<TransportAddress> boundAddresses, InetAddress publishInetAddress)
    {
        for (TransportAddress boundAddress : boundAddresses) {
            InetAddress boundInetAddress = boundAddress.address().getAddress();
            if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                return boundAddress.getPort();
            }
        }

        // if no matching boundAddress found, check if there is a unique port for all bound addresses
        final IntSet ports = new IntHashSet();
        for (TransportAddress boundAddress : boundAddresses) {
            ports.add(boundAddress.getPort());
        }
        if (ports.size() == 1) {
            return ports.iterator().next().value;
        }

        throw new BindHttpException(
                "Failed to auto-resolve psql publish port, multiple bound addresses " + boundAddresses +
                        " with distinct ports and none of them matched the publish address (" + publishInetAddress + "). ");
    }

    // TODO: make sure this annotation works.
    @PreDestroy
    protected void close()
    {
        for (Channel channel : serverChannels) {
            channel.close().awaitUninterruptibly();
        }
        serverChannels.clear();
        if (bootstrap != null) {
            bootstrap = null;
        }

        if (openChannels != null) {
            openChannels.close();
            openChannels = null;
        }
        LOGGER.info("close all channels.");
    }

    public long openConnections()
    {
        return openChannels == null ? 0L : openChannels.numberOfOpenChannels();
    }

    public long totalConnections()
    {
        return openChannels == null ? 0L : openChannels.totalChannels();
    }

    private Settings toWireProtocolSettings()
    {
        return Settings.builder()
                .put("network.bind_host", "0.0.0.0")
                .put("network.publish_host", "0.0.0.0")
                .build();
    }
}
