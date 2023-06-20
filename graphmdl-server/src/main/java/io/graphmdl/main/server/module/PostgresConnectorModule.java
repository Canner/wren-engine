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

package io.graphmdl.main.server.module;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.graphmdl.base.sql.SqlConverter;
import io.graphmdl.connector.postgres.PostgresClient;
import io.graphmdl.connector.postgres.PostgresConfig;
import io.graphmdl.main.connector.postgres.PostgresMetadata;
import io.graphmdl.main.connector.postgres.PostgresPreAggregationService;
import io.graphmdl.main.connector.postgres.PostgresSqlConverter;
import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.main.pgcatalog.builder.PgCatalogTableBuilder;
import io.graphmdl.main.pgcatalog.builder.PgFunctionBuilder;
import io.graphmdl.main.pgcatalog.builder.PostgresPgCatalogTableBuilder;
import io.graphmdl.main.pgcatalog.builder.PostgresPgFunctionBuilder;
import io.graphmdl.main.pgcatalog.regtype.PgMetadata;
import io.graphmdl.main.pgcatalog.regtype.PostgresPgMetadata;
import io.graphmdl.preaggregation.PreAggregationService;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class PostgresConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(PostgresClient.class).in(Scopes.SINGLETON);
        binder.bind(Metadata.class).to(PostgresMetadata.class).in(Scopes.SINGLETON);
        binder.bind(SqlConverter.class).to(PostgresSqlConverter.class).in(Scopes.SINGLETON);
        binder.bind(PgCatalogTableBuilder.class).to(PostgresPgCatalogTableBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgFunctionBuilder.class).to(PostgresPgFunctionBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgMetadata.class).to(PostgresPgMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PreAggregationService.class).to(PostgresPreAggregationService.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PostgresConfig.class);
    }
}
