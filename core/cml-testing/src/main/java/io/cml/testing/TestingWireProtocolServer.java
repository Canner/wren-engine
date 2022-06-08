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

package io.cml.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.cml.server.module.BigQueryConnectorModule;
import io.cml.server.module.PostgresWireProtocolModule;
import io.cml.wireprotocol.PostgresNetty;
import io.cml.wireprotocol.ssl.EmptyTlsDataProvider;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

import static io.cml.PostgresWireProtocolConfig.CANNERFLOW_PG_WIRE_PROTOCOL_PORT;

public class TestingWireProtocolServer
        implements Closeable
{
    private final Injector injector;
    private final Closer closer = Closer.create();

    public static Builder builder()
    {
        return new Builder();
    }

    private TestingWireProtocolServer(Map<String, String> requiredConfigs)
            throws IOException
    {
        Map<String, String> requiredConfigProps = new HashMap<>();
        requiredConfigProps.put(CANNERFLOW_PG_WIRE_PROTOCOL_PORT, String.valueOf(randomPort()));
        requiredConfigProps.putAll(requiredConfigs);

        Bootstrap app = new Bootstrap(ImmutableList.of(
                new PostgresWireProtocolModule(new EmptyTlsDataProvider()),
                new BigQueryConnectorModule()));

        injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfigs)
                .quiet()
                .initialize();

        closer.register(() -> injector.getInstance(LifeCycleManager.class).stop());
    }

    protected HostAndPort getHostAndPort()
    {
        return injector.getInstance(PostgresNetty.class).getHostAndPort();
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

        public TestingWireProtocolServer build()
        {
            try {
                return new TestingWireProtocolServer(configs);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
