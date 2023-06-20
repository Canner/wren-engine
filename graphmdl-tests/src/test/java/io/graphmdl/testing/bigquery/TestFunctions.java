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

package io.graphmdl.testing.bigquery;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFunctions
        extends AbstractWireProtocolTestWithBigQuery
{
    @DataProvider
    public Object[][] functions()
    {
        return new Object[][] {
                {"select array_recv('abc')", null, false},
                {"select array_in('{1,2,3')", null, false},
                // TODO: handle function return type or argument type is array
                //  https://github.com/Canner/canner-metric-layer/issues/76
                // {"select array_out(array[1,2])", null, false},
                {"select pg_relation_size(1)", null, false},
                {"select pg_relation_size(1, 'abc')", null, false},
                {"select current_schemas(false)", "pg_catalog", true},
                // TODO: fix current_database()
                //  https://github.com/Canner/canner-metric-layer/issues/75
                // {"select current_database()", "", false}
                {"select pg_get_expr('test', 1)", "", false},
                {"select pg_get_expr('test', 1, true)", "", false},
                // TODO: Which expected value should be ? 1.0 or 1
                // We use PARSE_AS_DECIMAL in WireProtocolSession#parse as default, all decimal literal will be parsed as Decimal type
                {"select trunc(1.1)", "1", false},
                {"select format_type(null, 1)", null, false},
                {"select format_type(1000, 1)", "_bool", false},
                {"select format_type(0, 1)", "???", false},
                {"select substr('testString', 0, 4)", "test", false},
                {"select concat('T.P.', ' ', 'Bar')", "T.P. Bar", false},
                {"select pg_get_function_result(7751334321673795072)", "int8", false}, // pg_relation_size__int4_varchar___int8
                {"select pg_get_function_result(2155180082033071319)", "varchar", false}, // current_database___varchar
                {"select regexp_like('pg_temp_table', '^pg_temp_')", "t", false},
                {"select date_trunc('year', '2023-03-30')", "2023-01-01", false},
                {"select date_trunc('day', timestamp '2023-03-30 18:00:00')", "2023-03-30 00:00:00.000000", false},
                {"SELECT to_char(TIMESTAMP '2023-06-13 09:17:04.859462', 'YYYY-MM-DD HH24:MI:SS.MS TZ') to_char", "2023-06-13 09:17:04.859 UTC", false},
                {"select information_schema._pg_expandarray(array[1, 2, 3])", "(1,1)", false},
        };
    }

    @Test(dataProvider = "functions")
    public void testJdbcQuery(String sql, String expected, boolean isArrayResult)
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement(sql);
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            if (isArrayResult) {
                assertThat(((String[]) resultSet.getArray(1).getArray())[0]).isEqualTo(expected);
            }
            else {
                assertThat(resultSet.getString(1)).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testNow()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery("select now()");
            assertThat(resultSet.next()).isTrue();
        }
    }
}
