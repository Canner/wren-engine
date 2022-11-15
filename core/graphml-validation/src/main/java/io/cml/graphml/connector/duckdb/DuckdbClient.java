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

package io.cml.graphml.connector.duckdb;

import io.cml.graphml.connector.Client;
import io.cml.graphml.connector.ColumnDescription;
import io.cml.graphml.connector.jdbc.JdbcRecordIterator;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.cml.graphml.connector.jdbc.JdbcTypeMapping.toGraphMLType;

public final class DuckdbClient
        implements Client
{
    private final Connection duckDBConnection;

    public DuckdbClient()
    {
        try {
            // The instance will be cleared after the process end. We don't need to
            // close this connection.
            this.duckDBConnection = DriverManager.getConnection("jdbc:duckdb:");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Object[]> query(String sql)
    {
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            return new JdbcRecordIterator(resultSet);
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    @Override
    public Iterator<ColumnDescription> describe(String sql)
    {
        // TODO: DuckDB 0.5.1 exists some issue about handling describe statement result.
        //  Before [duckdb#4796](https://github.com/duckdb/duckdb/pull/4799) released,
        //  execute query with `LIMIT 1` to get the ResultSetMetadata.
        String dryRunSql = sql + " LIMIT 1";
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(dryRunSql);
            ResultSet resultSet = statement.getResultSet();
            return new ColumnMetadataIterator(resultSet.getMetaData());
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    @Override
    public void queryDDL(String sql)
    {
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(sql);
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    @Override
    public List<String> listTables()
    {
        try (Connection connection = createConnection()) {
            ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null);
            List<String> names = new ArrayList<>();
            while (resultSet.next()) {
                String tableName = resultSet.getString(3);
                names.add(tableName);
            }
            return names;
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    static class ColumnMetadataIterator
            implements Iterator<ColumnDescription>
    {
        private final ResultSetMetaData metaData;
        private final int totalCount;
        private int index = 1;

        protected ColumnMetadataIterator(ResultSetMetaData metaData)
                throws SQLException
        {
            this.metaData = metaData;
            this.totalCount = metaData.getColumnCount();
        }

        @Override
        public boolean hasNext()
        {
            return index <= totalCount;
        }

        @Override
        public ColumnDescription next()
        {
            try {
                return new ColumnDescription(
                        metaData.getColumnName(index),
                        toGraphMLType(JDBCType.valueOf(metaData.getColumnType(index++))));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        // Refer to the official doc, if we want to create multiple read-write connections,
        // to the same database in-memory database instance, we can use the custom `duplicate()` method.
        // https://duckdb.org/docs/api/java

        return ((DuckDBConnection) duckDBConnection).duplicate();
    }
}
