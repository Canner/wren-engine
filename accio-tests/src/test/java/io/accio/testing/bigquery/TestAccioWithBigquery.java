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

package io.accio.testing.bigquery;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestAccioWithBigquery
        extends AbstractWireProtocolTestWithBigQuery
{
    @Override
    protected Optional<String> getAccioMDLPath()
    {
        return Optional.of(getClass().getClassLoader().getResource("tpch_mdl.json").getPath());
    }

    @DataProvider
    public Object[][] queryModel()
    {
        return new Object[][] {
                {"SELECT * FROM Orders"},
                {"SELECT * FROM Orders WHERE orderkey > 100"},
                {"SELECT * FROM Orders a JOIN Customer b ON a.custkey = b.custkey"},
                {"SELECT * FROM Orders WHERE nation_name IS NOT NULL"}
        };
    }

    @Test(dataProvider = "queryModel")
    public void testQueryModel(String sql)
    {
        assertThatNoException().isThrownBy(() -> {
            try (Connection connection = createConnection()) {
                PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet resultSet = stmt.executeQuery();
                resultSet.next();
            }
        });
    }

    @Test
    public void testQueryOnlyModelColumn()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select * from Orders limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getInt("orderkey"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("custkey"));
            assertThatNoException().isThrownBy(() -> resultSet.getString("orderstatus"));
            assertThatNoException().isThrownBy(() -> resultSet.getString("totalprice"));
            assertThatNoException().isThrownBy(() -> resultSet.getString("nation_name"));
            assertThatThrownBy(() -> resultSet.getString("o_orderkey"))
                    .hasMessageMatching(".*The column name o_orderkey was not found in this ResultSet.*");
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }
    }

    @Test
    public void testQueryMetric()
            throws Exception
    {
        // test the TO_ONE relationship
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select customer, totalprice from Revenue limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getString("customer"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("totalprice"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }

        // test the TO_MANY relationship
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select custkey, totalprice from CustomerRevenue limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getString("custkey"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("totalprice"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }
    }

    // TODO: support metric roll up relationship
    @Test(enabled = false)
    void testQueryMetricRollup()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select custkey, totalprice from roll_up(Revenue, orderdate, YEAR) limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getInt("custkey"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("totalprice"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }
    }

    @Test
    public void testQueryCumulativeMetric()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select * from WeeklyRevenue");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getInt("totalprice"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(53);
        }
    }

    @Test
    public void testEnum()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select Status.F as f1");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThat(resultSet.getString("f1")).isEqualTo("F");
        }

        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select count(*) as totalcount from Orders where orderstatus = Status.O");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThat(resultSet.getInt("totalcount")).isEqualTo(7333);
        }
    }

    @Test
    public void testView()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select * from useModel limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getInt("totalprice"));
            int count = 1;
            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }

        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select * from useMetric limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getString("customer"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("totalprice"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }

        // TODO: support metric roll up relationship
        // try (Connection connection = createConnection()) {
        //     PreparedStatement stmt = connection.prepareStatement("select * from useMetricRollUp limit 100");
        //     ResultSet resultSet = stmt.executeQuery();
        //     resultSet.next();
        //     assertThatNoException().isThrownBy(() -> resultSet.getInt("custkey"));
        //     assertThatNoException().isThrownBy(() -> resultSet.getInt("totalprice"));
        //     int count = 1;
        //
        //     while (resultSet.next()) {
        //         count++;
        //     }
        //     assertThat(count).isEqualTo(100);
        // }

        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select * from useUseMetric limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getString("customer"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("totalprice"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }
    }

    @Test
    public void testQuerySqlReservedWord()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select \"order\" from Lineitem limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getObject(1));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }
    }

    @Test
    public void testQueryMacro()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select custkey_name, custkey_call_concat from Customer limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getObject(1));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }
    }
}
