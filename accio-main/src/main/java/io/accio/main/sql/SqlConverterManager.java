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

package io.accio.main.sql;

import io.accio.base.SessionContext;
import io.accio.base.config.AccioConfig;
import io.accio.base.config.ConfigManager;
import io.accio.base.sql.SqlConverter;
import io.accio.main.connector.bigquery.BigQuerySqlConverter;
import io.accio.main.connector.duckdb.DuckDBSqlConverter;
import io.accio.main.connector.postgres.PostgresSqlConverter;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public final class SqlConverterManager
        implements SqlConverter
{
    private final BigQuerySqlConverter bigQuerySqlConverter;
    private final PostgresSqlConverter postgresSqlConverter;
    private final DuckDBSqlConverter duckDBSqlConverter;
    private final ConfigManager configManager;
    private AccioConfig.DataSourceType dataSourceType;
    private SqlConverter delegate;

    @Inject
    public SqlConverterManager(
            ConfigManager configManager,
            BigQuerySqlConverter bigQuerySqlConverter,
            PostgresSqlConverter postgresSqlConverter,
            DuckDBSqlConverter duckDBSqlConverter)
    {
        this.configManager = requireNonNull(configManager, "configManager is null");
        this.bigQuerySqlConverter = requireNonNull(bigQuerySqlConverter, "bigQuerySqlConverter is null");
        this.postgresSqlConverter = requireNonNull(postgresSqlConverter, "postgresSqlConverter is null");
        this.duckDBSqlConverter = requireNonNull(duckDBSqlConverter, "duckDBSqlConverter is null");
        this.dataSourceType = requireNonNull(configManager.getConfig(AccioConfig.class).getDataSourceType(), "dataSourceType is null");
        changeDelegate(dataSourceType);
    }

    private void changeDelegate(AccioConfig.DataSourceType dataSourceType)
    {
        switch (dataSourceType) {
            case BIGQUERY:
                delegate = bigQuerySqlConverter;
                break;
            case POSTGRES:
                delegate = postgresSqlConverter;
                break;
            case DUCKDB:
                delegate = duckDBSqlConverter;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data source type: " + dataSourceType);
        }
    }

    public void reload()
    {
        if (dataSourceType != configManager.getConfig(AccioConfig.class).getDataSourceType()) {
            dataSourceType = configManager.getConfig(AccioConfig.class).getDataSourceType();
            changeDelegate(dataSourceType);
        }
    }

    @Override
    public String convert(String sql, SessionContext sessionContext)
    {
        return delegate.convert(sql, sessionContext);
    }
}
