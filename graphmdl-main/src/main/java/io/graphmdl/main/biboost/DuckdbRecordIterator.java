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
package io.graphmdl.main.biboost;

import com.google.common.collect.ImmutableList;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.type.PGType;
import io.graphmdl.connector.AutoCloseableIterator;
import io.graphmdl.connector.jdbc.JdbcRecordIterator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static io.graphmdl.connector.duckdb.DuckdbType.DUCKDB_TYPE;
import static java.util.Objects.requireNonNull;

public class DuckdbRecordIterator
        implements ConnectorRecordIterator
{
    private final List<PGType> types;
    private final AutoCloseableIterator<Object[]> recordIterator;

    public static DuckdbRecordIterator of(ResultSet resultSet)
            throws SQLException
    {
        return new DuckdbRecordIterator(resultSet);
    }

    private DuckdbRecordIterator(ResultSet resultSet)
            throws SQLException
    {
        requireNonNull(resultSet, "resultSet is null");
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        ImmutableList.Builder<PGType> typeBuilder = ImmutableList.builder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            int columnType = resultSetMetaData.getColumnType(i);
            PGType<?> pgType = DUCKDB_TYPE.toPGType(columnType);
            typeBuilder.add(pgType);
        }
        this.types = typeBuilder.build();
        this.recordIterator = new JdbcRecordIterator(resultSet);
    }

    @Override
    public List<PGType> getTypes()
    {
        return types;
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
        return recordIterator.next();
    }
}
