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

import com.google.common.collect.ImmutableMap;
import io.cml.testing.AbstractWireProtocolTest;
import io.cml.testing.TestingWireProtocolServer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFunctions
        extends AbstractWireProtocolTest
{
    @Override
    protected TestingWireProtocolServer createWireProtocolServer()
    {
        return TestingWireProtocolServer.builder()
                .setRequiredConfigs(
                        ImmutableMap.<String, String>builder()
                                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                                .put("bigquery.location", "asia-east1")
                                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                                .build())
                .build();
    }

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
                {"select trunc(1.1)", "1.0", false}
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
}
