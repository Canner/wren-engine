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

package io.accio.base.client.duckdb;

import com.google.common.collect.ImmutableList;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.accio.base.AccioException;
import io.accio.base.Parameter;
import io.accio.base.client.AutoCloseableIterator;
import io.accio.base.client.Client;
import io.accio.base.client.jdbc.JdbcRecordIterator;
import io.accio.base.metadata.ColumnMetadata;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.duckdb.DuckDBConnection;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static io.accio.base.client.duckdb.DuckdbTypes.toPGType;
import static io.accio.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;

public final class DuckdbClient
        implements Client
{
    private static final Logger LOG = Logger.get(DuckdbClient.class);
    private final DuckDBConfig duckDBConfig;
    private final HikariDataSource ds;

    @Inject
    public DuckdbClient(DuckDBConfig duckDBConfig)
    {
        try {
            // The instance will be cleared after the process end. We don't need to
            // close this connection
            Class.forName("org.duckdb.DuckDBDriver");
            DuckDBConnection duckDBConnection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            this.duckDBConfig = duckDBConfig;
            DuckDBDataSource dataSource = new DuckDBDataSource(duckDBConnection);
            HikariConfig config = new HikariConfig();
            config.setDataSource(dataSource);
            config.setPoolName("MY_POOL");
            config.setConnectionTimeout(10000);
            config.setMinimumIdle(10);
            config.setMaximumPoolSize(20);
            ds = new HikariDataSource(config);
            init();
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void init()
    {
        DataSize memoryLimit = duckDBConfig.getMemoryLimit();
        executeDDL(format("SET memory_limit='%s'", memoryLimit.toBytesValueString()));
        LOG.info("Set memory limit to %s", memoryLimit.toBytesValueString());
        executeDDL(format("SET temp_directory='%s'", duckDBConfig.getTempDirectory()));
        LOG.info("Set temp directory to %s", duckDBConfig.getTempDirectory());
        // TODO: Known issue: https://github.com/duckdb/duckdb/issues/10062
        // executeDDL(format("SET home_directory='%s'", duckDBConfig.getHomeDirectory()));
        // LOG.info("Set home directory to %s", duckDBConfig.getHomeDirectory());
    }

    @Override
    public AutoCloseableIterator<Object[]> query(String sql)
    {
        try {
            return JdbcRecordIterator.of(this, sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AutoCloseableIterator<Object[]> query(String sql, List<Parameter> parameters)
    {
        try {
            return JdbcRecordIterator.of(this, sql, parameters);
        }
        catch (Exception e) {
            LOG.error(e, "Error executing DDL");
            throw new AccioException(GENERIC_USER_ERROR, e);
        }
    }

    @Override
    public List<ColumnMetadata> describe(String sql, List<Parameter> parameters)
    {
        try (Connection connection = createConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i).getValue());
            }
            // workaround for describe duckdb sql
            preparedStatement.execute();
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            int columnCount = metaData.getColumnCount();

            ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
            for (int i = 1; i <= columnCount; i++) {
                builder.add(ColumnMetadata.builder()
                        .setName(metaData.getColumnName(i))
                        .setType(toPGType(metaData.getColumnType(i)))
                        .build());
            }
            return builder.build();
        }
        catch (Exception e) {
            LOG.error(e, "Error executing DDL");
            throw new AccioException(GENERIC_USER_ERROR, e);
        }
    }

    @Override
    public void executeDDL(String sql)
    {
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(sql);
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    // for canner use
    public void executeDDL(String sql, List<Object> parameters)
    {
        try (Connection connection = createConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                statement.setObject(i + 1, parameters.get(i));
            }
            statement.execute();
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    @Override
    public List<String> listTables()
    {
        try (Connection connection = createConnection();
                ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
            List<String> names = new ArrayList<>();
            while (resultSet.next()) {
                String tableName = resultSet.getString(3);
                names.add(tableName);
            }
            return names;
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    public void dropTableQuietly(String tableName)
    {
        try {
            executeDDL(format("BEGIN TRANSACTION;DROP TABLE IF EXISTS %s;COMMIT;", tableName));
        }
        catch (Exception e) {
            LOG.error(e, "Failed to drop table %s", tableName);
        }
    }

    @Override
    public Connection createConnection()
            throws SQLException
    {
        return ds.getConnection();
    }

    public DuckDBConfig getDuckDBConfig()
    {
        return duckDBConfig;
    }
}
