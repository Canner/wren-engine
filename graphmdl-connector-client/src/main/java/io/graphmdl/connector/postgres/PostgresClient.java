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

package io.graphmdl.connector.postgres;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.airlift.log.Logger;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.Parameter;
import io.graphmdl.base.client.AutoCloseableIterator;
import io.graphmdl.base.client.Client;
import io.graphmdl.base.metadata.ColumnMetadata;
import io.graphmdl.base.metadata.SchemaTableName;
import io.graphmdl.base.metadata.TableMetadata;
import io.graphmdl.base.type.IntervalType;
import io.graphmdl.base.type.PGArray;
import io.graphmdl.base.type.PGType;
import org.joda.time.Period;
import org.postgresql.util.PGInterval;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.graphmdl.connector.postgres.PostgresJdbcType.toPGType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class PostgresClient
        implements Client
{
    private static final Logger LOG = Logger.get(PostgresClient.class);
    private final PostgresConfig postgresConfig;

    @Inject
    public PostgresClient(PostgresConfig postgresConfig)
    {
        this.postgresConfig = requireNonNull(postgresConfig, "postgresConfig is null");
    }

    public List<TableMetadata> listTable(String schemaName)
    {
        try (Connection connection = createConnection()) {
            ResultSet resultSet = connection.getMetaData().getColumns(null, schemaName, null, null);
            ListMultimap<SchemaTableName, ColumnMetadata> metadataBuilder = ArrayListMultimap.create();
            while (resultSet.next()) {
                LOG.debug("type: %s", resultSet.getString("TYPE_NAME"));
                metadataBuilder.put(new SchemaTableName(resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_NAME")),
                        ColumnMetadata.builder()
                                .setName(resultSet.getString("COLUMN_NAME"))
                                .setType(toPGType(resultSet.getString("TYPE_NAME")))
                                .build());
            }
            return metadataBuilder.asMap().entrySet().stream()
                    .map(entry -> {
                        TableMetadata.Builder builder = TableMetadata.builder(entry.getKey());
                        entry.getValue().forEach(builder::column);
                        return builder.build();
                    }).collect(toUnmodifiableList());
        }
        catch (Exception e) {
            LOG.error(e, "Error executing listTables");
            throw new GraphMDLException(GENERIC_USER_ERROR, e);
        }
    }

    @Override
    public AutoCloseableIterator<Object[]> query(String sql)
    {
        try {
            return PostgresRecordIterator.of(this, sql);
        }
        catch (Exception e) {
            LOG.error(e, "Error executing query");
            throw new GraphMDLException(GENERIC_USER_ERROR, e);
        }
    }

    @Override
    public AutoCloseableIterator<Object[]> query(String sql, List<Parameter> parameters)
    {
        try {
            return PostgresRecordIterator.of(this, sql, parameters);
        }
        catch (Exception e) {
            LOG.error(e, "Error executing query");
            throw new GraphMDLException(GENERIC_USER_ERROR, e);
        }
    }

    @Override
    public void executeDDL(String sql)
    {
        try (Connection connection = createConnection()) {
            connection.createStatement().execute(sql);
        }
        catch (Exception e) {
            LOG.error(e, "Error executing DDL");
            throw new GraphMDLException(GENERIC_USER_ERROR, e);
        }
    }

    @Override
    public List<ColumnMetadata> describe(String sql, List<Parameter> parameters)
    {
        try (Connection connection = createConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            setParameter(preparedStatement, parameters);
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            int columnCount = metaData.getColumnCount();

            ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
            for (int i = 1; i <= columnCount; i++) {
                builder.add(ColumnMetadata.builder()
                        .setName(metaData.getColumnName(i))
                        .setType(toPGType(metaData.getColumnTypeName(i)))
                        .build());
            }
            return builder.build();
        }
        catch (Exception e) {
            LOG.error(e, "Error executing describe");
            throw new GraphMDLException(GENERIC_USER_ERROR, e);
        }
    }

    @Override
    public List<String> listTables()
    {
        try (Connection connection = createConnection()) {
            ResultSet resultSet = connection.getMetaData().getColumns(null, null, null, null);
            ListMultimap<SchemaTableName, ColumnMetadata> metadataBuilder = ArrayListMultimap.create();
            while (resultSet.next()) {
                LOG.info("type: %s", resultSet.getString("TYPE_NAME"));
                metadataBuilder.put(new SchemaTableName(resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_NAME")),
                        ColumnMetadata.builder()
                                .setName(resultSet.getString("COLUMN_NAME"))
                                .setType(toPGType(resultSet.getString("TYPE_NAME")))
                                .build());
            }
            return metadataBuilder.asMap().entrySet().stream()
                    .map(entry -> {
                        TableMetadata.Builder builder = TableMetadata.builder(entry.getKey());
                        entry.getValue().forEach(builder::column);
                        return builder.build();
                    })
                    .map(TableMetadata::getTable)
                    .map(SchemaTableName::toString)
                    .collect(toUnmodifiableList());
        }
        catch (Exception e) {
            LOG.error(e, "Error executing listTables");
            throw new GraphMDLException(GENERIC_USER_ERROR, e);
        }
    }

    public Connection createConnection()
            throws SQLException
    {
        return DriverManager.getConnection(postgresConfig.getJdbcUrl(), postgresConfig.getUser(), postgresConfig.getPassword());
    }

    public static void setParameter(PreparedStatement preparedStatement, List<Parameter> parameters)
            throws SQLException
    {
        for (int i = 1; i <= parameters.size(); i++) {
            PGType<?> pgType = parameters.get(i - 1).getType();
            if (pgType == IntervalType.INTERVAL) {
                Period period = (Period) parameters.get(i - 1).getValue();
                preparedStatement.setObject(i, new PGInterval(period.getYears(), period.getMonths(), period.getDays(), period.getHours(), period.getMinutes(), period.getSeconds()));
                continue;
            }
            else if (pgType instanceof PGArray) {
                List<Object> values = (List<Object>) parameters.get(i - 1).getValue();
                preparedStatement.setArray(i, preparedStatement.getConnection().createArrayOf(((PGArray) pgType).getInnerType().typName(), values.toArray()));
                continue;
            }
            preparedStatement.setObject(i, parameters.get(i - 1).getValue());
        }
    }
}
