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

package io.accio.main.pgcatalog.builder;

import io.accio.base.config.AccioConfig;
import io.accio.base.config.ConfigManager;
import io.accio.base.pgcatalog.function.PgFunction;
import io.accio.main.connector.bigquery.BigQueryMetadata;
import io.accio.main.connector.duckdb.DuckDBMetadata;
import io.accio.main.connector.postgres.PostgresMetadata;
import io.accio.main.metadata.Metadata;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PgFunctionBuilderManager
{
    private static final Logger LOG = Logger.get(PgFunctionBuilderManager.class);
    private final ConfigManager configManager;
    private final BigQueryMetadata bigQueryMetadata;
    private final PostgresMetadata postgresMetadata;
    private final DuckDBMetadata duckDBMetadata;
    private AccioConfig.DataSourceType dataSourceType;
    private Metadata connector;

    @Inject
    public PgFunctionBuilderManager(
            ConfigManager configManager,
            BigQueryMetadata bigQueryMetadata,
            PostgresMetadata postgresMetadata,
            DuckDBMetadata duckDBMetadata)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        this.bigQueryMetadata = requireNonNull(bigQueryMetadata, "bigQueryMetadata is null");
        this.postgresMetadata = requireNonNull(postgresMetadata, "postgresMetadata is null");
        this.duckDBMetadata = requireNonNull(duckDBMetadata, "duckDBMetadata is null");
        this.dataSourceType = requireNonNull(configManager.getConfig(AccioConfig.class).getDataSourceType(), "dataSourceType is null");
        changeDataSourceType(dataSourceType);
    }

    public void createPgFunction(PgFunction pgFunction)
    {
        String sql = connector.getPgFunctionBuilder().generateCreateFunction(pgFunction);
        LOG.info("Creating or updating %s.%s: %s", connector.getPgCatalogName(), pgFunction.getName(), sql);
        connector.directDDL(sql);
        LOG.info("%s.%s has created or updated", connector.getPgCatalogName(), pgFunction.getName());
    }

    private synchronized void changeDataSourceType(AccioConfig.DataSourceType dataSourceType)
    {
        switch (dataSourceType) {
            case BIGQUERY:
                connector = bigQueryMetadata;
                break;
            case POSTGRES:
                connector = postgresMetadata;
                break;
            case DUCKDB:
                connector = duckDBMetadata;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data source type: " + dataSourceType);
        }
    }

    public void reload()
    {
        if (dataSourceType != configManager.getConfig(AccioConfig.class).getDataSourceType()) {
            dataSourceType = configManager.getConfig(AccioConfig.class).getDataSourceType();
            changeDataSourceType(dataSourceType);
        }
    }
}
