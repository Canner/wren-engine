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

package io.wren.main.connector.postgres;

import com.google.common.collect.ImmutableList;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.type.PGType;
import io.wren.connector.postgres.PostgresJdbcType;
import io.wren.connector.postgres.PostgresRecordIterator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PostgresConnectorRecordIterator
        implements ConnectorRecordIterator
{
    // TODO: Implement ConnectorRecordIterator instead of JdbcRecordIterator
    private final PostgresRecordIterator internalIterator;
    private final List<Column> columns;

    public PostgresConnectorRecordIterator(PostgresRecordIterator internalIterator)
            throws SQLException
    {
        this.internalIterator = requireNonNull(internalIterator, "internalIterator is null");
        ResultSetMetaData resultSetMetaData = internalIterator.getResultSetMetaData();
        ImmutableList.Builder<Column> columnBuilder = ImmutableList.builder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnType = resultSetMetaData.getColumnTypeName(i);
            PGType<?> pgType = PostgresJdbcType.toPGType(columnType);
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
        internalIterator.close();
    }

    @Override
    public boolean hasNext()
    {
        return internalIterator.hasNext();
    }

    @Override
    public Object[] next()
    {
        return internalIterator.next();
    }
}
