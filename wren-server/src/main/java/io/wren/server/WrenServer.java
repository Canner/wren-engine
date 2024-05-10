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

package io.wren.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import io.wren.base.config.PostgresWireProtocolConfig;
import io.wren.cache.CacheModule;
import io.wren.main.WrenModule;
import io.wren.main.server.Server;
import io.wren.main.wireprotocol.ssl.EmptyTlsDataProvider;
import io.wren.server.module.BigQueryConnectorModule;
import io.wren.server.module.DuckDBConnectorModule;
import io.wren.server.module.MainModule;
import io.wren.server.module.PostgresConnectorModule;
import io.wren.server.module.PostgresWireProtocolModule;
import io.wren.server.module.SQLGlotModule;
import io.wren.server.module.SnowflakeConnectorModule;
import io.wren.server.module.WebModule;

import static io.airlift.configuration.ConditionalModule.conditionalModule;

public class WrenServer
        extends Server
{
    public static void main(String[] args)
    {
        new WrenServer().start();
    }

    @Override
    protected Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of(
                new NodeModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new EventModule(),
                new MainModule(),
                new BigQueryConnectorModule(),
                new PostgresConnectorModule(),
                new DuckDBConnectorModule(),
                new SnowflakeConnectorModule(),
                new WrenModule(),
                conditionalModule(PostgresWireProtocolConfig.class, PostgresWireProtocolConfig::isPgWireProtocolEnabled, new CacheModule()),
                conditionalModule(PostgresWireProtocolConfig.class, PostgresWireProtocolConfig::isPgWireProtocolEnabled, new PostgresWireProtocolModule(new EmptyTlsDataProvider())),
                new WebModule(),
                new SQLGlotModule());
    }
}
