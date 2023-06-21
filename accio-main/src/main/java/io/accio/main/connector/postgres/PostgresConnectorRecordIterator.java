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

package io.accio.main.connector.postgres;

import com.google.common.collect.ImmutableList;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.type.PGType;
import io.accio.connector.postgres.PostgresJdbcType;
import io.accio.connector.postgres.PostgresRecordIterator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PostgresConnectorRecordIterator
        implements ConnectorRecordIterator
{
    // TODO: Implement ConnectorRecordIterator instead of JdbcRecordIterator
    private final PostgresRecordIterator internalIterator;
    private final List<PGType> types;

    public PostgresConnectorRecordIterator(PostgresRecordIterator internalIterator)
            throws SQLException
    {
        this.internalIterator = requireNonNull(internalIterator, "internalIterator is null");
        ResultSetMetaData resultSetMetaData = internalIterator.getResultSetMetaData();
        ImmutableList.Builder<PGType> typeBuilder = ImmutableList.builder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnType = resultSetMetaData.getColumnTypeName(i);
            PGType<?> pgType = PostgresJdbcType.toPGType(columnType);
            typeBuilder.add(pgType);
        }
        this.types = typeBuilder.build();
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
