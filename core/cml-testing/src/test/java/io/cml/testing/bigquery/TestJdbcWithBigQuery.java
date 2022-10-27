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

package io.cml.testing.bigquery;

import io.cml.testing.AbstractJdbcTest;
import io.cml.testing.JdbcTesting;
import io.cml.testing.TestingCmlServer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestJdbcWithBigQuery
        extends AbstractJdbcTest
        implements BigQueryTesting, JdbcTesting
{
    @Override
    protected TestingCmlServer createCmlServer()
    {
        return createCmlServerWithBigQuery();
    }

    @DataProvider
    public Object[][] jdbcQuery()
    {
        return new Object[][] {
                {"SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'"},
                {"SELECT\n" +
                        "  t.typname\n" +
                        ", t.oid\n" +
                        "FROM\n" +
                        "  (\"canner-cml\".pg_catalog.pg_type t\n" +
                        "INNER JOIN \"canner-cml\".pg_catalog.pg_namespace n ON (t.typnamespace = n.oid))\n" +
                        "WHERE ((n.nspname <> 'pg_toast') AND ((t.typrelid = 0) OR (SELECT (c.relkind = 'c') \"?column?\"\n" +
                        "FROM\n" +
                        "  \"canner-cml\".pg_catalog.pg_class c\n" +
                        "WHERE (c.oid = t.typrelid)\n" +
                        ")))"},
                {"SELECT 1, 2, 3"},
                {"SELECT array[1,2,3][1]"},
                {"select current_schemas(false)[1]"},
                {"select typinput = 1, typoutput = 1, typreceive = 1 from \"canner-cml\".pg_catalog.pg_type"},
                {"select * from unnest(generate_array(1, 10)) t(col_1)"},
                {"select * from unnest(array[1,2,3]) t(col_1)"},
                {"SELECT\n" +
                        "s.r\n" +
                        ", current_schemas(false)[s.r] nspname\n" +
                        "FROM\n" +
                        "UNNEST(generate_array(1, array_upper(current_schemas(false), 1))) s (r)"},
        };
    }

    /**
     * In this test, we only check the query used by jdbc can be parsed and executed.
     * We don't care whether the result is correct.
     */
    @Test(dataProvider = "jdbcQuery")
    public void testJdbcQuery(String sql)
            throws SQLException
    {
        try (Connection connection = createConnection(server())) {
            PreparedStatement stmt = connection.prepareStatement(sql);
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
        }
    }
}
