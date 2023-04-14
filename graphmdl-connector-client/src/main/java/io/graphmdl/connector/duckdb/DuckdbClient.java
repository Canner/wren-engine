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

package io.graphmdl.connector.duckdb;

import io.graphmdl.base.Parameter;
import io.graphmdl.connector.AutoCloseableIterator;
import io.graphmdl.connector.Client;
import io.graphmdl.connector.ColumnDescription;
import io.graphmdl.connector.jdbc.JdbcRecordIterator;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static io.graphmdl.connector.jdbc.JdbcTypeMapping.toGraphMDLType;

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
    public AutoCloseableIterator<Object[]> query(String sql)
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
    public AutoCloseableIterator<ColumnDescription> describe(String sql)
    {
        String describeSql = "describe " + sql;
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(describeSql);
            ResultSet resultSet = statement.getResultSet();
            return new ColumnMetadataIterator(resultSet);
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    public ResultSet prepareStatement(String sql, List<Parameter> parameters)
    {
        try (Connection connection = createConnection()) {
            PreparedStatement statement = connection.prepareStatement(sql);
            for (int i = 0; i < parameters.size(); i++) {
                statement.setObject(i + 1, parameters.get(i).getValue());
            }

            return statement.executeQuery();
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    @Override
    public void executeDDL(String sql)
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
        try (Connection connection = createConnection();
                ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
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
            implements AutoCloseableIterator<ColumnDescription>
    {
        private final ResultSet resultSet;

        private boolean hasNext;

        private ColumnDescription nowBuffer;

        public ColumnMetadataIterator(ResultSet resultSet)
                throws SQLException
        {
            this.resultSet = resultSet;

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
        public ColumnDescription next()
        {
            ColumnDescription nowRecord = nowBuffer;
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

        // The schema of a describe query in duckDB:
        // │ column_name ┆ column_type ┆ null ┆ key ┆ default ┆ extra │
        // ╞═════════════╪═════════════╪══════╪═════╪═════════╪═══════╡
        // │ 1           ┆ INTEGER     ┆ YES  ┆     ┆         ┆       │

        private ColumnDescription getCurrentRecord()
                throws SQLException
        {
            return new ColumnDescription(
                    resultSet.getString(1),
                    toGraphMDLType(JDBCType.valueOf(resultSet.getString(2))));
        }

        @Override
        public void close()
                throws Exception
        {
            this.resultSet.close();
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
