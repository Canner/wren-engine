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
import com.google.inject.Scopes;
import io.accio.base.sql.SqlConverter;
import io.accio.cache.CacheService;
import io.accio.main.connector.bigquery.BigQueryCacheService;
import io.accio.main.connector.bigquery.BigQueryMetadata;
import io.accio.main.connector.bigquery.BigQuerySqlConverter;
import io.accio.main.connector.duckdb.DuckDBMetadata;
import io.accio.main.metadata.Metadata;
import io.accio.main.pgcatalog.builder.BigQueryPgCatalogTableBuilder;
import io.accio.main.pgcatalog.builder.BigQueryPgFunctionBuilder;
import io.accio.main.pgcatalog.builder.DuckDBFunctionBuilder;
import io.accio.main.pgcatalog.builder.PgCatalogTableBuilder;
import io.accio.main.pgcatalog.builder.PgFunctionBuilder;
import io.accio.main.pgcatalog.builder.PgMetastoreFunctionBuilder;
import io.accio.main.pgcatalog.regtype.PgMetadata;
import io.accio.main.pgcatalog.regtype.PostgresPgMetadata;
import io.accio.main.wireprotocol.PgMetastore;
import io.airlift.configuration.AbstractConfigurationAwareModule;

public class BigQueryConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(Metadata.class).to(BigQueryMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PgCatalogTableBuilder.class).to(BigQueryPgCatalogTableBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgFunctionBuilder.class).to(BigQueryPgFunctionBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgMetastoreFunctionBuilder.class).to(DuckDBFunctionBuilder.class).in(Scopes.SINGLETON);
        binder.bind(PgMetadata.class).to(PostgresPgMetadata.class).in(Scopes.SINGLETON);
        binder.bind(SqlConverter.class).to(BigQuerySqlConverter.class).in(Scopes.SINGLETON);
        binder.bind(PgMetastore.class).to(DuckDBMetadata.class).in(Scopes.SINGLETON);
        binder.bind(CacheService.class).to(BigQueryCacheService.class).in(Scopes.SINGLETON);
    }
}
