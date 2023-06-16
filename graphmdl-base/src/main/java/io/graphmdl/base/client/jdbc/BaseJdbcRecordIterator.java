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
package io.graphmdl.base.client.jdbc;

import io.graphmdl.base.client.AutoCloseableIterator;
import io.graphmdl.base.client.Client;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public abstract class BaseJdbcRecordIterator<T>
        implements AutoCloseableIterator<T>
{
    private final Connection connection;
    private final PreparedStatement statement;
    protected final ResultSet resultSet;
    private final ResultSetMetaData resultSetMetaData;
    protected final int columnCount;

    private boolean hasNext;

    private T nowBuffer;

    public BaseJdbcRecordIterator(Client client, String sql)
            throws SQLException
    {
        this(client, sql, emptyList());
    }

    public BaseJdbcRecordIterator(Client client, String sql, List<Object> parameters)
            throws SQLException
    {
        requireNonNull(client, "client is null");
        connection = client.createConnection();
        statement = connection.prepareStatement(sql);
        for (int i = 0; i < parameters.size(); i++) {
            statement.setObject(i + 1, parameters.get(i));
        }
        resultSet = statement.executeQuery();

        this.resultSetMetaData = resultSet.getMetaData();
        this.columnCount = resultSetMetaData.getColumnCount();

        hasNext = resultSet.next();
        if (hasNext) {
            nowBuffer = getCurrentRecord();
        }
    }

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public T next()
    {
        T nowRecord = nowBuffer;
        try {
            hasNext = resultSet.next();
            if (hasNext) {
                nowBuffer = getCurrentRecord();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return nowRecord;
    }

    @Override
    public void close()
            throws Exception
    {
        // use try with resources to close everything properly
        try (Connection connection = this.connection;
                Statement statement = this.statement;
                ResultSet resultSet = this.resultSet) {
            if (statement != null) {
                try {
                    // Trying to cancel running statement as close() may not do it
                    statement.cancel();
                }
                catch (SQLException ignored) {
                    // statement already closed or cancel is not supported
                }
            }
            if (connection != null && resultSet != null) {
                if (!resultSet.isAfterLast()) {
                    connection.abort(directExecutor());
                }
            }
        }
        catch (SQLException | RuntimeException e) {
            // ignore exception from close
        }
    }

    public ResultSetMetaData getResultSetMetaData()
    {
        return resultSetMetaData;
    }

    public abstract T getCurrentRecord()
            throws SQLException;
}
