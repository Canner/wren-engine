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

package io.wren.main.connector.snowflake;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.wren.base.Column;
import io.wren.base.Parameter;
import io.wren.base.WrenException;
import io.wren.base.config.SnowflakeConfig;
import io.wren.connector.postgres.PostgresClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static io.wren.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.wren.main.connector.snowflake.SnowflakeTypes.toPGType;
import static java.util.Objects.requireNonNull;

public class SnowflakeClient
{
    private static final Logger LOG = Logger.get(PostgresClient.class);

    private final SnowflakeConfig config;

    public SnowflakeClient(SnowflakeConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    public Connection createConnection()
            throws SQLException
    {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        Properties props = new Properties();
        props.setProperty("user", config.getUser());
        props.setProperty("password", config.getPassword());
        config.getDatabase().ifPresent(database -> props.setProperty("db", database));
        config.getSchema().ifPresent(schema -> props.setProperty("schema", schema));
        config.getWarehouse().ifPresent(warehouse -> props.setProperty("warehouse", warehouse));
        config.getRole().ifPresent(role -> props.setProperty("role", role));

        return DriverManager.getConnection(config.getJdbcUrl(), props);
    }

    public void execute(String sql)
    {
        try (Connection connection = createConnection()) {
            connection.createStatement().execute(sql);
        }
        catch (Exception e) {
            LOG.error(e, "Error executing DDL");
            throw new WrenException(GENERIC_USER_ERROR, e);
        }
    }

    public SnowflakeRecordIterator query(String sql, List<Parameter> parameters)
    {
        try {
            return SnowflakeRecordIterator.of(createConnection(), sql, parameters);
        }
        catch (Exception e) {
            LOG.error(e, "Error executing query");
            throw new WrenException(GENERIC_USER_ERROR, e);
        }
    }

    public List<Column> describe(String sql, List<Parameter> parameters)
    {
        try (Connection connection = createConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                statement.setObject(i + 1, parameters.get(i).getValue());
            }

            return buildColumns(statement.getMetaData());
        }
        catch (SQLException e) {
            LOG.error(e, "Error executing describe");
            throw new WrenException(GENERIC_USER_ERROR, e);
        }
    }

    public static List<Column> buildColumns(ResultSetMetaData metaData)
            throws SQLException
    {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            builder.add(new Column(metaData.getColumnName(i), toPGType(metaData.getColumnType(i))));
        }
        return builder.build();
    }
}
