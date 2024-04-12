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

import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.wren.main.connector.snowflake.SnowflakeClient.buildColumns;

public class SnowflakeRecordIterator
        implements ConnectorRecordIterator
{
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;
    private final int columnCount;
    private final List<Column> columns;

    private boolean hasNext;

    static SnowflakeRecordIterator of(Connection connection, String sql, List<Parameter> parameters)
            throws SQLException
    {
        return new SnowflakeRecordIterator(connection, sql, parameters);
    }

    private SnowflakeRecordIterator(Connection connection, String sql, List<Parameter> parameters)
            throws SQLException
    {
        try {
            this.connection = connection;

            statement = connection.prepareStatement(sql);

            for (int i = 0; i < parameters.size(); i++) {
                statement.setObject(i + 1, parameters.get(i).getValue());
            }

            resultSet = statement.executeQuery();

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            this.columnCount = resultSetMetaData.getColumnCount();

            this.columns = buildColumns(resultSetMetaData);

            hasNext = resultSet.next();
        }
        catch (SQLException e) {
            close();
            throw e;
        }
    }

    @Override
    public List<Column> getColumns()
    {
        return columns;
    }

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public Object[] next()
    {
        if (!hasNext) {
            throw new NoSuchElementException();
        }
        try {
            Object[] currentResult = getCurrentRecord();
            hasNext = resultSet.next();
            return currentResult;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        // use try with resources to close everything properly
        try (connection; statement; resultSet) {
            if (statement != null) {
                try {
                    // Trying to cancel running statement as close() may not do it
                    statement.cancel();
                }
                catch (SQLException ignored) {
                    // statement already closed or cancel is not supported
                }
            }
            if (connection != null && resultSet != null && !resultSet.isAfterLast()) {
                connection.abort(directExecutor());
            }
        }
        catch (SQLException | RuntimeException e) {
            // ignore exception from close
        }
    }

    private Object[] getCurrentRecord()
            throws SQLException
    {
        List<Object> record = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnType = metaData.getColumnType(i);
            record.add(dataHandle(columnType, i));
        }
        return record.toArray();
    }

    private Object dataHandle(int columnType, int i)
            throws SQLException
    {
        // TODO: Unsure data type handle is correct
        if (columnType == Types.BLOB) {
            Blob blob = resultSet.getBlob(i);
            return blob.getBytes(0, (int) blob.length());
        }
        if (columnType == Types.SMALLINT) {
            return resultSet.getShort(i);
        }
        if (columnType == Types.TIMESTAMP) {
            return resultSet.getTimestamp(i).toLocalDateTime();
        }
        if (columnType == Types.DATE) {
            return resultSet.getDate(i).toLocalDate();
        }
        return resultSet.getObject(i);
    }
}
