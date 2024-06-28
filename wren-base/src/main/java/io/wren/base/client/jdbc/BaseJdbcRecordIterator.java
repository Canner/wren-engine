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
package io.wren.base.client.jdbc;

import io.wren.base.Parameter;
import io.wren.base.client.AutoCloseableIterator;
import io.wren.base.client.Client;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public abstract class BaseJdbcRecordIterator<T>
        implements AutoCloseableIterator<T>
{
    private final Connection connection;
    protected final PreparedStatement statement;
    protected final ResultSet resultSet;
    private final ResultSetMetaData resultSetMetaData;
    protected final int columnCount;

    private boolean hasNext;

    public BaseJdbcRecordIterator(Client client, String sql, List<Parameter> parameters)
            throws SQLException
    {
        requireNonNull(client, "client is null");
        connection = client.createConnection();
        try {
            statement = connection.prepareStatement(sql);
            setParameter(parameters);
            resultSet = statement.executeQuery();

            this.resultSetMetaData = resultSet.getMetaData();
            this.columnCount = resultSetMetaData.getColumnCount();

            hasNext = resultSet.next();
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
    }

    protected void setParameter(List<Parameter> parameters)
            throws SQLException
    {
        for (int i = 0; i < parameters.size(); i++) {
            statement.setObject(i + 1, parameters.get(i).getValue());
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
        if (!hasNext) {
            throw new NoSuchElementException();
        }
        T currentResult;
        try {
            currentResult = getCurrentRecord();
            // move to next row
            hasNext = resultSet.next();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return currentResult;
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
