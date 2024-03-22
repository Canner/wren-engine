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

package io.wren.main.sql;

import io.wren.base.SessionContext;
import io.wren.base.config.ConfigManager;
import io.wren.base.config.WrenConfig;
import io.wren.base.sql.SqlConverter;
import io.wren.main.connector.bigquery.BigQuerySqlConverter;
import io.wren.main.connector.duckdb.DuckDBSqlConverter;
import io.wren.main.connector.postgres.PostgresSqlConverter;
import io.wren.sql.converter.SQLGlotConverter;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public final class SqlConverterManager
        implements SqlConverter
{
    private final BigQuerySqlConverter bigQuerySqlConverter;
    private final PostgresSqlConverter postgresSqlConverter;
    private final DuckDBSqlConverter duckDBSqlConverter;
    private final SQLGlotConverter sqlGlotConverter;
    private final ConfigManager configManager;
    private WrenConfig.DataSourceType dataSourceType;
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
        this.sqlGlotConverter = new SQLGlotConverter();
        this.dataSourceType = requireNonNull(configManager.getConfig(WrenConfig.class).getDataSourceType(), "dataSourceType is null");
        changeDelegate(dataSourceType);
    }

    private void changeDelegate(WrenConfig.DataSourceType dataSourceType)
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
        if (dataSourceType != configManager.getConfig(WrenConfig.class).getDataSourceType()) {
            dataSourceType = configManager.getConfig(WrenConfig.class).getDataSourceType();
            changeDelegate(dataSourceType);
        }
    }

    @Override
    public String convert(String sql, SessionContext sessionContext)
    {
        if (configManager.getConfig(WrenConfig.class).getEnableSQLGlot()) {
            return sqlGlotConverter.convert(sql, sessionContext);
        }
        return delegate.convert(sql, sessionContext);
    }
}
