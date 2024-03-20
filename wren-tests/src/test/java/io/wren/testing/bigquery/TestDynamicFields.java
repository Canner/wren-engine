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

package io.wren.testing.bigquery;

import com.google.common.collect.ImmutableMap;
import io.wren.testing.TestingWrenServer;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.wren.base.Utils.randomIntString;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestDynamicFields
        extends AbstractWireProtocolTestWithBigQuery
{
    @Override
    protected TestingWrenServer createWrenServer()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("bigquery.metadata.schema.prefix", format("test_%s_", randomIntString()))
                .put("wren.datasource.type", "bigquery")
                .put("wren.experimental-enable-dynamic-fields", "true");

        try {
            Path dir = Files.createTempDirectory(getWrenDirectory());
            Files.copy(Path.of(getClass().getClassLoader().getResource("tpch_mdl.json").getPath()), dir.resolve("mdl.json"));
            properties.put("wren.directory", dir.toString());
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testDynamicMetric()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            // select one dimension and measure
            PreparedStatement stmt = connection.prepareStatement("SELECT customer, totalprice FROM CustomerDailyRevenue WHERE customer = 'Customer#000000048'");
            ResultSet resultSet = stmt.executeQuery();
            List<List<Object>> actual = toList(resultSet);

            stmt = connection.prepareStatement("SELECT c.name, SUM(o.totalprice) FROM Orders o LEFT JOIN Customer c ON o.custkey = c.custkey\n" +
                    "WHERE c.name = 'Customer#000000048' GROUP BY 1");
            resultSet = stmt.executeQuery();
            List<List<Object>> expected = toList(resultSet);

            assertThat(actual).isEqualTo(expected);

            // select two dimensions and measure
            stmt = connection.prepareStatement("SELECT customer, date, totalprice FROM CustomerDailyRevenue WHERE customer = 'Customer#000000048' ORDER BY 1, 2");
            resultSet = stmt.executeQuery();
            actual = toList(resultSet);

            stmt = connection.prepareStatement("SELECT c.name, o.orderdate, SUM(o.totalprice) FROM Orders o LEFT JOIN Customer c ON o.custkey = c.custkey\n" +
                    "WHERE c.name = 'Customer#000000048' GROUP BY 1, 2 ORDER BY 1, 2");
            resultSet = stmt.executeQuery();
            expected = toList(resultSet);

            assertThat(actual).isEqualTo(expected);
        }
    }

    private List<List<Object>> toList(ResultSet resultSet)
            throws SQLException
    {
        List<List<Object>> result = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                row.add(resultSet.getObject(i + 1));
            }
            result.add(row);
        }
        return result;
    }
}
