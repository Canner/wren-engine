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
package io.wren.main.connector.duckdb;

import com.google.common.collect.ImmutableList;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.base.client.AutoCloseableIterator;
import io.wren.base.client.Client;
import io.wren.base.client.jdbc.JdbcRecordIterator;
import org.duckdb.DuckDBArray;
import org.duckdb.DuckDBResultSet;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DuckdbRecordIterator
        implements ConnectorRecordIterator
{
    private final List<Column> columns;
    private final AutoCloseableIterator<Object[]> recordIterator;

    public static DuckdbRecordIterator of(Client client, String sql, List<Parameter> parameters)
            throws SQLException
    {
        return new DuckdbRecordIterator(client, sql, parameters);
    }

    private DuckdbRecordIterator(Client client, String sql, List<Parameter> parameters)
            throws SQLException
    {
        requireNonNull(client, "client is null");
        requireNonNull(sql, "sql is null");
        requireNonNull(parameters, "parameters is null");
        JdbcRecordIterator jdbcRecordIterator = JdbcRecordIterator.of(client, sql, parameters);
        this.recordIterator = jdbcRecordIterator;

        ResultSetMetaData resultSetMetaData = jdbcRecordIterator.getResultSetMetaData();
        ImmutableList.Builder<Column> columnBuilder = ImmutableList.builder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            columnBuilder.add(new Column(resultSetMetaData.getColumnName(i), resultSetMetaData.getColumnTypeName(i)));
        }
        this.columns = columnBuilder.build();
    }

    @Override
    public List<Column> getColumns()
    {
        return columns;
    }

    @Override
    public void close()
            throws Exception
    {
        recordIterator.close();
    }

    @Override
    public boolean hasNext()
    {
        return recordIterator.hasNext();
    }

    @Override
    public Object[] next()
    {
        Object[] record = recordIterator.next();
        return IntStream.range(0, record.length)
                .mapToObj(index -> convertValue(columns.get(index).getType(), record[index]))
                .toArray();
    }

    private Object convertValue(String type, Object value)
    {
        try {
            if (value == null) {
                return null;
            }
            if (type.endsWith("[]")) {
                if (value instanceof DuckDBArray array) {
                    return Arrays.stream((Object[]) array.getArray())
                            .map(innerVal -> convertValue(type.substring(0, type.length() - 2), innerVal))
                            .collect(toList());
                }
                return Arrays.stream((Object[]) value)
                        .map(innerVal -> convertValue(type.substring(0, type.length() - 2), innerVal))
                        .collect(toList());
            }
            switch (type) {
                case "TIMESTAMP":
                    return ((Timestamp) value).toLocalDateTime();
                case "BLOB":
                    if (value instanceof DuckDBResultSet.DuckDBBlobResult) {
                        DuckDBResultSet.DuckDBBlobResult blob = (DuckDBResultSet.DuckDBBlobResult) value;
                        return blob.getBytes(1, (int) blob.length());
                    }
                case "JSON":
                    return value.toString();
            }
            return value;
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unsupported value: " + value, e);
        }
    }
}
