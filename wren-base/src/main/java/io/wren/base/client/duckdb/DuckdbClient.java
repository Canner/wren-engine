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

package io.wren.base.client.duckdb;

import com.google.common.collect.ImmutableList;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.wren.base.Parameter;
import io.wren.base.WrenException;
import io.wren.base.client.AutoCloseableIterator;
import io.wren.base.client.Client;
import io.wren.base.client.jdbc.JdbcRecordIterator;
import io.wren.base.metadata.ColumnMetadata;
import io.wren.base.metadata.StandardErrorCode;
import io.wren.base.type.PGType;
import org.duckdb.DuckDBConnection;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.wren.base.client.duckdb.DuckdbTypes.toPGType;
import static java.lang.String.format;

public final class DuckdbClient
        implements Client
{
    private static final Logger LOG = Logger.get(DuckdbClient.class);
    private final DuckDBConfig duckDBConfig;
    private final CacheStorageConfig cacheStorageConfig;
    private final DuckDBSettingSQL duckDBSettingSQL;
    private DuckDBConnection duckDBConnection;
    private HikariDataSource connectionPool;

    public DuckdbClient(
            DuckDBConfig duckDBConfig,
            @Nullable CacheStorageConfig cacheStorageConfig,
            @Nullable DuckDBSettingSQL duckDBSettingSQL)
    {
        this.duckDBConfig = duckDBConfig;
        this.cacheStorageConfig = cacheStorageConfig;
        this.duckDBSettingSQL = duckDBSettingSQL;
        init();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private void init()
    {
        try {
            // The instance will be cleared after the process end. We don't need to
            // close this connection
            Class.forName("org.duckdb.DuckDBDriver");
            duckDBConnection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            initPool();
            if (duckDBSettingSQL != null) {
                if (duckDBSettingSQL.getInitSQL() != null) {
                    executeDDL(duckDBSettingSQL.getInitSQL());
                }
            }
            else {
                DataSize memoryLimit = duckDBConfig.getMemoryLimit();
                executeDDL(format("SET memory_limit='%s'", memoryLimit.toBytesValueString()));
                LOG.info("Set memory limit to %s", memoryLimit.toBytesValueString());
                executeDDL(format("SET temp_directory='%s'", duckDBConfig.getTempDirectory()));
                LOG.info("Set temp directory to %s", duckDBConfig.getTempDirectory());
            }
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void initPool()
    {
        connectionPool = new HikariDataSource(getHikariConfig(duckDBConfig, cacheStorageConfig, duckDBConnection, duckDBSettingSQL));
    }

    private static HikariConfig getHikariConfig(
            DuckDBConfig duckDBConfig,
            CacheStorageConfig cacheStorageConfig,
            DuckDBConnection duckDBConnection,
            DuckDBSettingSQL duckDBSettingSQL)
    {
        DuckDBDataSource dataSource = new DuckDBDataSource(duckDBConnection);
        HikariConfig config = new HikariConfig();
        config.setDataSource(dataSource);
        config.setPoolName("DUCKDB_POOL");
        config.setConnectionTimeout(10000);
        config.setMinimumIdle(duckDBConfig.getMaxConcurrentTasks());
        // remain some query slots for metadata queries
        config.setMaximumPoolSize(duckDBConfig.getMaxConcurrentTasks() + duckDBConfig.getMaxConcurrentMetadataQueries());
        String initSql = buildConnectionInitSql(duckDBSettingSQL, cacheStorageConfig, duckDBConfig);
        config.setConnectionInitSql(initSql);
        return config;
    }

    private static String buildConnectionInitSql(DuckDBSettingSQL duckDBSettingSQL, CacheStorageConfig cacheStorageConfig, DuckDBConfig duckDBConfig)
    {
        List<String> sql = new ArrayList<>();
        if (duckDBSettingSQL != null) {
            if (duckDBSettingSQL.getSessionSQL() != null) {
                sql.add(duckDBSettingSQL.getSessionSQL());
            }
        }
        else {
            sql.add("SET search_path = 'main'");
            if (cacheStorageConfig instanceof DuckdbS3StyleStorageConfig) {
                DuckdbS3StyleStorageConfig duckdbS3StyleStorageConfig = (DuckdbS3StyleStorageConfig) cacheStorageConfig;
                sql.add(format("SET s3_endpoint='%s'", duckdbS3StyleStorageConfig.getEndpoint()));
                sql.add(format("SET s3_url_style='%s'", duckdbS3StyleStorageConfig.getUrlStyle()));
            }
            sql.add(format("SET home_directory='%s'", duckDBConfig.getHomeDirectory()));
        }
        return String.join(";", sql);
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
            throw new WrenException(StandardErrorCode.GENERIC_USER_ERROR, e);
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
            ResultSetMetaData metaData = preparedStatement.getResultSet().getMetaData();
            int columnCount = metaData.getColumnCount();

            ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
            for (int i = 1; i <= columnCount; i++) {
                PGType<?> type = toPGType(metaData, i);
                builder.add(ColumnMetadata.builder()
                        .setName(metaData.getColumnName(i))
                        .setType(type)
                        .build());
            }
            return builder.build();
        }
        catch (Exception e) {
            LOG.error(e, "Error executing DDL: %s", sql);
            throw new WrenException(StandardErrorCode.GENERIC_USER_ERROR, e);
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
            LOG.error("Failed SQL: %s", sql);
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

    @Override
    public Connection createConnection()
            throws SQLException
    {
        return connectionPool.getConnection();
    }

    @Override
    public void close()
    {
        try {
            connectionPool.close();
            duckDBConnection.close();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void closeAndInitPool()
    {
        connectionPool.close();
        initPool();
    }

    public static class Builder
    {
        private DuckDBConfig duckDBConfig;
        private CacheStorageConfig cacheStorageConfig;
        private DuckDBSettingSQL duckDBSettingSQL;

        public Builder setDuckDBConfig(DuckDBConfig duckDBConfig)
        {
            this.duckDBConfig = duckDBConfig;
            return this;
        }

        public Builder setCacheStorageConfig(CacheStorageConfig cacheStorageConfig)
        {
            this.cacheStorageConfig = cacheStorageConfig;
            return this;
        }

        public Builder setDuckDBSettingSQL(DuckDBSettingSQL duckDBSettingSQL)
        {
            this.duckDBSettingSQL = duckDBSettingSQL;
            return this;
        }

        public DuckdbClient build()
        {
            return new DuckdbClient(duckDBConfig, cacheStorageConfig, duckDBSettingSQL);
        }

        public Optional<DuckdbClient> buildSafely()
        {
            try {
                return Optional.of(build());
            }
            catch (Exception e) {
                LOG.error(e, "Failed to build DuckdbClient");
                return Optional.empty();
            }
        }
    }
}
