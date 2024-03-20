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

package io.wren.main.metadata;

import io.trino.sql.tree.QualifiedName;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.base.config.ConfigManager;
import io.wren.base.config.WrenConfig;
import io.wren.base.metadata.TableMetadata;
import io.wren.connector.StorageClient;
import io.wren.main.connector.bigquery.BigQueryMetadata;
import io.wren.main.connector.duckdb.DuckDBMetadata;
import io.wren.main.connector.postgres.PostgresMetadata;
import io.wren.main.pgcatalog.builder.PgFunctionBuilder;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class MetadataManager
        implements Metadata
{
    private final ConfigManager configManager;
    private final BigQueryMetadata bigQueryMetadata;
    private final PostgresMetadata postgresMetadata;
    private final DuckDBMetadata duckDBMetadata;

    private WrenConfig.DataSourceType dataSourceType;
    private Metadata delegate;

    @Inject
    public MetadataManager(
            ConfigManager configManager,
            BigQueryMetadata bigQueryMetadata,
            PostgresMetadata postgresMetadata,
            DuckDBMetadata duckDBMetadata)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        this.bigQueryMetadata = requireNonNull(bigQueryMetadata, "bigQueryMetadata is null");
        this.postgresMetadata = requireNonNull(postgresMetadata, "postgresMetadata is null");
        this.duckDBMetadata = requireNonNull(duckDBMetadata, "duckDBMetadata is null");
        this.dataSourceType = requireNonNull(configManager.getConfig(WrenConfig.class).getDataSourceType(), "dataSourceType is null");
        changeDelegate(dataSourceType);
    }

    private synchronized void changeDelegate(WrenConfig.DataSourceType dataSourceType)
    {
        switch (dataSourceType) {
            case BIGQUERY:
                delegate = bigQueryMetadata;
                break;
            case POSTGRES:
                delegate = postgresMetadata;
                break;
            case DUCKDB:
                delegate = duckDBMetadata;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data source type: " + dataSourceType);
        }
    }

    @Override
    public void createSchema(String name)
    {
        delegate.createSchema(name);
    }

    @Override
    public boolean isSchemaExist(String name)
    {
        return delegate.isSchemaExist(name);
    }

    @Override
    public void dropSchemaIfExists(String name)
    {
        delegate.dropSchemaIfExists(name);
    }

    @Override
    public List<String> listSchemas()
    {
        return delegate.listSchemas();
    }

    @Override
    public List<TableMetadata> listTables(String schemaName)
    {
        return delegate.listTables(schemaName);
    }

    @Override
    public List<String> listFunctionNames(String schemaName)
    {
        return delegate.listFunctionNames(schemaName);
    }

    @Override
    public QualifiedName resolveFunction(String functionName, int numArgument)
    {
        return delegate.resolveFunction(functionName, numArgument);
    }

    @Override
    public String getDefaultCatalog()
    {
        return delegate.getDefaultCatalog();
    }

    @Override
    public void directDDL(String sql)
    {
        delegate.directDDL(sql);
    }

    @Override
    public ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters)
    {
        return delegate.directQuery(sql, parameters);
    }

    @Override
    public List<Column> describeQuery(String sql, List<Parameter> parameters)
    {
        return delegate.describeQuery(sql, parameters);
    }

    @Override
    public boolean isPgCompatible()
    {
        return delegate.isPgCompatible();
    }

    @Override
    public String getMetadataSchemaName()
    {
        return delegate.getMetadataSchemaName();
    }

    @Override
    public String getPgCatalogName()
    {
        return delegate.getPgCatalogName();
    }

    @Override
    public void reload()
    {
        WrenConfig.DataSourceType newDataSourceType = configManager.getConfig(WrenConfig.class).getDataSourceType();
        if (dataSourceType != newDataSourceType) {
            dataSourceType = newDataSourceType;
            delegate.close();
            changeDelegate(dataSourceType);
        }
        delegate.reload();
    }

    @Override
    public StorageClient getCacheStorageClient()
    {
        return delegate.getCacheStorageClient();
    }

    @Override
    public PgFunctionBuilder getPgFunctionBuilder()
    {
        return delegate.getPgFunctionBuilder();
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
