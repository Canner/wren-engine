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

package io.accio.server.module;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.accio.base.client.ForCache;
import io.accio.base.client.ForConnector;
import io.accio.base.client.duckdb.DuckDBConnectorConfig;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.main.connector.duckdb.DuckDBMetadata;
import io.accio.main.connector.duckdb.DuckDBSqlConverter;
import io.accio.main.connector.postgres.PostgresCacheService;
import io.accio.main.metadata.Metadata;
import io.accio.main.pgcatalog.builder.DuckDBFunctionBuilder;
import io.accio.main.pgcatalog.builder.PgMetastoreFunctionBuilder;
import io.accio.main.pgcatalog.regtype.PgMetadata;
import io.accio.main.pgcatalog.regtype.PostgresPgMetadata;
import io.accio.main.web.DuckDBResource;
import io.accio.main.wireprotocol.PgMetastore;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class DuckDBConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DuckDBConnectorConfig.class);
        binder.bind(DuckdbClient.class).annotatedWith(ForConnector.class).to(DuckdbClient.class).in(Scopes.SINGLETON);
        binder.bind(DuckDBSqlConverter.class).in(Scopes.SINGLETON);
        binder.bind(DuckDBFunctionBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgMetastoreFunctionBuilder.class).to(DuckDBFunctionBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgMetadata.class).to(PostgresPgMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PostgresCacheService.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(DuckDBResource.class);
    }

    @Provides
    @Singleton
    public static Metadata provideMetadata(@ForConnector DuckdbClient duckdbClient)
    {
        return new DuckDBMetadata(duckdbClient);
    }

    @Provides
    @Singleton
    public static PgMetastore providePgMetastore(@ForCache DuckdbClient duckdbClient)
    {
        return new DuckDBMetadata(duckdbClient);
    }
}
