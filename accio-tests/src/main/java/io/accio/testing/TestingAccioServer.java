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
import io.accio.cache.CacheModule;
import io.accio.main.AccioConfig;
import io.accio.main.AccioModule;
import io.accio.main.pgcatalog.PgCatalogManager;
import io.accio.main.server.module.BigQueryConnectorModule;
import io.accio.main.server.module.PostgresConnectorModule;
import io.accio.main.server.module.PostgresWireProtocolModule;
import io.accio.main.server.module.WebModule;
import io.accio.main.wireprotocol.PostgresNetty;
import io.accio.main.wireprotocol.ssl.EmptyTlsDataProvider;
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
import java.util.HashMap;
import java.util.Map;

import static io.accio.main.AccioConfig.DataSourceType.BIGQUERY;
import static io.accio.main.AccioConfig.DataSourceType.POSTGRES;
import static io.accio.main.PostgresWireProtocolConfig.PG_WIRE_PROTOCOL_PORT;
import static io.airlift.configuration.ConditionalModule.conditionalModule;

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
                new PostgresWireProtocolModule(new EmptyTlsDataProvider()),
                conditionalModule(AccioConfig.class, config -> config.getDataSourceType().equals(BIGQUERY), new BigQueryConnectorModule()),
                conditionalModule(AccioConfig.class, config -> config.getDataSourceType().equals(POSTGRES), new PostgresConnectorModule()),
                new AccioModule(),
                new CacheModule(),
                new WebModule()));

        injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfigProps)
                .quiet()
                .initialize();

        PgCatalogManager pgCatalogManager = injector.getInstance(PgCatalogManager.class);
        pgCatalogManager.initPgCatalog();

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
                return new TestingAccioServer(configs);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
