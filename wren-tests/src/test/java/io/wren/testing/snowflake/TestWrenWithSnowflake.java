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

package io.wren.testing.snowflake;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestWrenWithSnowflake
        extends AbstractWireProtocolTestWithSnowflake
{
    @BeforeClass
    public void init()
            throws Exception
    {
        testingSQLGlotServer = closer.register(prepareSQLGlot());
        wrenServer = closer.register(createWrenServer());
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws IOException
    {
        closer.close();
    }

    @DataProvider
    public Object[][] queryModel()
    {
        return new Object[][] {
                {"SELECT * FROM ORDERS"},
                {"SELECT * FROM ORDERS WHERE ORDERKEY > 100"},
                {"SELECT * FROM ORDERS a JOIN CUSTOMER b ON a.CUSTKEY = b.CUSTKEY"},
                {"SELECT * FROM ORDERS WHERE NATION_NAME IS NOT NULL"}
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
            PreparedStatement stmt = connection.prepareStatement("select * from ORDERS limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getInt("ORDERKEY"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("CUSTKEY"));
            assertThatNoException().isThrownBy(() -> resultSet.getString("ORDERSTATUS"));
            assertThatNoException().isThrownBy(() -> resultSet.getString("TOTALPRICE"));
            assertThatNoException().isThrownBy(() -> resultSet.getString("NATION_NAME"));
            assertThatThrownBy(() -> resultSet.getString("O_ORDERKEY"))
                    .hasMessageMatching(".*The column name O_ORDERKEY was not found in this ResultSet.*");
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
            PreparedStatement stmt = connection.prepareStatement("select CUSTOMER, TOTALPRICE from REVENUE limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getString("CUSTOMER"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("TOTALPRICE"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }

        // test the TO_MANY relationship
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select CUSTKEY, TOTALPRICE from CUSTOMER_REVENUE limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getString("CUSTKEY"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("TOTALPRICE"));
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
            PreparedStatement stmt = connection.prepareStatement("select CUSTKEY, TOTALPRICE from ROLLUP(REVENUE, ORDERDATE, YEAR) limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getInt("CUSTKEY"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("TOTALPRICE"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }
    }

    @Test(enabled = false, description = "Snowflake does not support generate timestamp array dynamically")
    public void testQueryCumulativeMetric()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select * from WEEKLY_REVENUE");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getInt("TOTALPRICE"));
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
            PreparedStatement stmt = connection.prepareStatement("select STATUS.F as F1");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThat(resultSet.getString("F1")).isEqualTo("F");
        }

        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select COUNT(*) as TOTALCOUNT from ORDERS where ORDERSTATUS = STATUS.O");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThat(resultSet.getInt("TOTALCOUNT")).isEqualTo(732044);
        }
    }

    @Test
    public void testView()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select * from USE_MODEL limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getInt("TOTALPRICE"));
            int count = 1;
            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }

        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select * from USE_METRIC limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getString("CUSTOMER"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("TOTALPRICE"));
            int count = 1;

            while (resultSet.next()) {
                count++;
            }
            assertThat(count).isEqualTo(100);
        }

        // TODO: support metric roll up relationship
        // try (Connection connection = createConnection()) {
        //     PreparedStatement stmt = connection.prepareStatement("select * from USE_METRIC_ROLLUP limit 100");
        //     ResultSet resultSet = stmt.executeQuery();
        //     resultSet.next();
        //     assertThatNoException().isThrownBy(() -> resultSet.getInt("CUSTKEY"));
        //     assertThatNoException().isThrownBy(() -> resultSet.getInt("TOTALPRICE"));
        //     int count = 1;
        //
        //     while (resultSet.next()) {
        //         count++;
        //     }
        //     assertThat(count).isEqualTo(100);
        // }

        try (Connection connection = createConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select * from USE_USE_METRIC limit 100");
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            assertThatNoException().isThrownBy(() -> resultSet.getString("CUSTOMER"));
            assertThatNoException().isThrownBy(() -> resultSet.getInt("TOTALPRICE"));
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
            PreparedStatement stmt = connection.prepareStatement("select CONSTANT from LINEITEM limit 100");
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
            PreparedStatement stmt = connection.prepareStatement("select CUSTKEY_NAME, CUSTKEY_CALL_CONCAT from CUSTOMER limit 100");
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
    public void testQueryCte()
    {
        assertThatNoException().isThrownBy(() -> {
            try (Connection connection = createConnection()) {
                PreparedStatement stmt = connection.prepareStatement("with CTE as (select * from ORDERS) select * from CTE");
                ResultSet resultSet = stmt.executeQuery();
                resultSet.next();
            }

            // test type correction rewrite
            try (Connection connection = createConnection()) {
                PreparedStatement stmt = connection.prepareStatement("with CTE as (select * from ORDERS) select * from CTE join ORDERS on CTE.ORDERKEY = ORDERS.ORDERKEY");
                ResultSet resultSet = stmt.executeQuery();
                resultSet.next();
            }
        });
    }

    @Test
    public void testCaseSensitive()
    {
        assertThatNoException().isThrownBy(() -> {
            try (Connection connection = createConnection()) {
                PreparedStatement stmt = connection.prepareStatement("SELECT \"caseSensitive\" FROM ORDERS");
                ResultSet resultSet = stmt.executeQuery();
                resultSet.next();
            }
        });
    }
}
