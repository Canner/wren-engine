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

package io.cml.graphml.connector.jdbc;

import io.cml.graphml.connector.AutoCloseableIterator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class JdbcRecordIterator
        extends AutoCloseableIterator<Object[]>
{
    private final ResultSet resultSet;
    private final int columnCount;

    private boolean hasNext;

    private Object[] nowBuffer;

    public JdbcRecordIterator(ResultSet resultSet)
            throws SQLException
    {
        this.resultSet = requireNonNull(resultSet);
        ResultSetMetaData metaData = resultSet.getMetaData();
        this.columnCount = metaData.getColumnCount();

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
    public Object[] next()
    {
        Object[] nowRecord = nowBuffer;
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
        this.resultSet.close();
    }

    private Object[] getCurrentRecord()
            throws SQLException
    {
        List<Object> builder = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            builder.add(resultSet.getObject(i));
        }
        return builder.toArray();
    }
}
