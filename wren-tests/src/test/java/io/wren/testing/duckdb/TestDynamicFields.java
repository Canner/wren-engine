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

package io.wren.testing.duckdb;

import com.google.common.collect.ImmutableMap;
import io.wren.base.dto.Manifest;
import io.wren.main.web.dto.QueryResultDto;
import io.wren.testing.AbstractTestFramework;
import io.wren.testing.TestingWrenServer;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.base.config.WrenConfig.WREN_ENABLE_DYNAMIC_FIELDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicFields
        extends AbstractTestFramework
{
    private Manifest manifest;

    @Override
    protected TestingWrenServer createWrenServer()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put(WREN_DATASOURCE_TYPE, DUCKDB.name())
                .put(WREN_ENABLE_DYNAMIC_FIELDS, "true");

        try {
            manifest = MANIFEST_JSON_CODEC.fromJson(Files.readString(Path.of(getClass().getClassLoader().getResource("tpch_mdl.json").getPath())));
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
        // select one dimension and measure
        QueryResultDto actual = query(manifest, "SELECT customer, totalprice FROM CustomerDailyRevenue WHERE customer = 'Customer#000000048'");
        QueryResultDto expected = query(manifest, "SELECT c.name as customer, SUM(o.totalprice) as totalprice FROM Orders o LEFT JOIN Customer c ON o.custkey = c.custkey\n" +
                "WHERE c.name = 'Customer#000000048' GROUP BY 1");

        assertThat(actual).isEqualTo(expected);

        // select two dimensions and measure
        actual = query(manifest, "SELECT customer, date, totalprice FROM CustomerDailyRevenue WHERE customer = 'Customer#000000048' ORDER BY 1, 2");
        expected = query(manifest, "SELECT c.name as customer, o.orderdate as date, SUM(o.totalprice) as totalprice FROM Orders o LEFT JOIN Customer c ON o.custkey = c.custkey\n" +
                        "WHERE c.name = 'Customer#000000048' GROUP BY 1, 2 ORDER BY 1, 2");
        assertThat(actual).isEqualTo(expected);
    }
}
