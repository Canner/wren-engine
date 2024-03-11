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

package io.accio.testing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.accio.base.client.ForCache;
import io.accio.base.client.ForConnector;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.base.config.AccioConfig;
import io.accio.cache.CacheModule;
import io.accio.main.AccioModule;
import io.accio.main.wireprotocol.PostgresNetty;
import io.accio.main.wireprotocol.ssl.EmptyTlsDataProvider;
import io.accio.server.module.BigQueryConnectorModule;
import io.accio.server.module.DuckDBConnectorModule;
import io.accio.server.module.MainModule;
import io.accio.server.module.PostgresConnectorModule;
import io.accio.server.module.PostgresWireProtocolModule;
import io.accio.server.module.WebModule;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.accio.base.config.AccioConfig.DataSourceType.DUCKDB;
import static io.accio.base.config.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_PORT;

public class TestingAccioServer
        implements Closeable
{
    private static final String HTTP_SERVER_PORT = "http-server.http.port";
    private static final String NODE_ENVIRONMENT = "node.environment";
    private final Injector injector;
    private final Closer closer = Closer.create();

    public static Builder builder()
    {
        return new Builder();
    }

    private TestingAccioServer(Map<String, String> requiredConfigs)
            throws IOException
    {
        Map<String, String> requiredConfigProps = new HashMap<>();
        requiredConfigProps.put(PG_WIRE_PROTOCOL_PORT, String.valueOf(randomPort()));
        requiredConfigProps.put(HTTP_SERVER_PORT, String.valueOf(randomPort()));
        requiredConfigProps.put(NODE_ENVIRONMENT, "test");

        requiredConfigProps.putAll(requiredConfigs);

        Bootstrap app = new Bootstrap(ImmutableList.<Module>of(
                new TestingHttpServerModule(),
                new NodeModule(),
                new JsonModule(),
                new JaxrsModule(),
                new EventModule(),
                new MainModule(),
                new PostgresWireProtocolModule(new EmptyTlsDataProvider()),
                new BigQueryConnectorModule(),
                new PostgresConnectorModule(),
                new DuckDBConnectorModule(),
                new AccioModule(),
                new CacheModule(),
                new WebModule()));

        injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfigProps)
                .quiet()
                .initialize();

        closer.register(() -> injector.getInstance(Key.get(DuckdbClient.class, ForCache.class)).close());
        if (injector.getInstance(AccioConfig.class).getDataSourceType().equals(DUCKDB)) {
            closer.register(() -> injector.getInstance(Key.get(DuckdbClient.class, ForConnector.class)).close());
        }
        closer.register(() -> injector.getInstance(LifeCycleManager.class).stop());
    }

    @VisibleForTesting
    public HostAndPort getPgHostAndPort()
    {
        return injector.getInstance(PostgresNetty.class).getHostAndPort();
    }

    public URI getHttpServerBasedUrl()
    {
        return injector.getInstance(TestingHttpServer.class).getBaseUrl();
    }

    public <T> T getInstance(Key<T> key)
    {
        return injector.getInstance(key);
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    private static int randomPort()
            throws IOException
    {
        // ServerSocket(0) results in availability of a free random port
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    public static class Builder
    {
        private Map<String, String> configs = new HashMap<>();

        public Builder setRequireConfig(String key, String value)
        {
            this.configs.put(key, value);
            return this;
        }

        public Builder setRequiredConfigs(Map<String, String> configs)
        {
            this.configs = configs;
            return this;
        }

        public TestingAccioServer build()
        {
            try {
                Path config = Files.createTempDirectory("config").resolve("config.properties");
                System.setProperty("config", config.toString());
                Properties properties = new Properties();
                properties.putAll(configs);
                properties.store(Files.newBufferedWriter(config), "TestingAccioServer");
                return new TestingAccioServer(configs);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
