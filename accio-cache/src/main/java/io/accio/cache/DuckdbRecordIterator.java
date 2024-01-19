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
package io.accio.cache;

import com.google.common.collect.ImmutableList;
import io.accio.base.Column;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.Parameter;
import io.accio.base.client.AutoCloseableIterator;
import io.accio.base.client.Client;
import io.accio.base.client.jdbc.JdbcRecordIterator;
import io.accio.base.type.PGArray;
import io.accio.base.type.PGType;
import io.accio.base.type.TimestampType;
import org.duckdb.DuckDBArray;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static io.accio.base.client.duckdb.DuckdbTypes.toPGType;
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
            int columnType = resultSetMetaData.getColumnType(i);
            PGType<?> pgType = toPGType(columnType);
            columnBuilder.add(new Column(resultSetMetaData.getColumnName(i), pgType));
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

    private Object convertValue(PGType<?> pgType, Object value)
    {
        try {
            if (pgType instanceof TimestampType) {
                return ((Timestamp) value).toLocalDateTime();
            }
            if (pgType.equals(PGArray.VARCHAR_ARRAY)) {
                return Arrays.asList((Object[]) ((DuckDBArray) value).getArray()).stream().map(Object::toString).collect(toList());
            }
            return value;
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unsupported value: " + value, e);
        }
    }
}
