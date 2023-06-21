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

package io.graphmdl.main.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import io.graphmdl.main.GraphMDLConfig;
import io.graphmdl.main.GraphMDLModule;
import io.graphmdl.main.pgcatalog.PgCatalogManager;
import io.graphmdl.main.server.module.BigQueryConnectorModule;
import io.graphmdl.main.server.module.PostgresConnectorModule;
import io.graphmdl.main.server.module.PostgresWireProtocolModule;
import io.graphmdl.main.server.module.WebModule;
import io.graphmdl.main.wireprotocol.ssl.EmptyTlsDataProvider;
import io.graphmdl.preaggregation.PreAggregationModule;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.graphmdl.main.GraphMDLConfig.DataSourceType.BIGQUERY;
import static io.graphmdl.main.GraphMDLConfig.DataSourceType.POSTGRES;

public class GraphMDLServer
        extends Server
{
    public static void main(String[] args)
    {
        new GraphMDLServer().start();
    }

    @Override
    protected void configure(Injector injector)
    {
        PgCatalogManager pgCatalogManager = injector.getInstance(PgCatalogManager.class);
        pgCatalogManager.initPgCatalog();
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
                new PostgresWireProtocolModule(new EmptyTlsDataProvider()),
                conditionalModule(GraphMDLConfig.class, config -> config.getDataSourceType().equals(BIGQUERY), new BigQueryConnectorModule()),
                conditionalModule(GraphMDLConfig.class, config -> config.getDataSourceType().equals(POSTGRES), new PostgresConnectorModule()),
                new GraphMDLModule(),
                new PreAggregationModule(),
                new WebModule());
    }
}
