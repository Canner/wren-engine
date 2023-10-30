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

package io.accio.sqlrewrite;

import io.accio.base.AccioMDL;
import io.accio.base.dto.DateSpine;
import io.accio.base.dto.TimeUnit;
import io.accio.testing.AbstractTestFramework;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioTypes.DATE;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.CumulativeMetric.cumulativeMetric;
import static io.accio.base.dto.Measure.measure;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Window.window;
import static io.accio.sqlrewrite.AccioSqlRewrite.ACCIO_SQL_REWRITE;
import static io.accio.sqlrewrite.MetricViewSqlRewrite.METRIC_VIEW_SQL_REWRITE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCumulativeMetric
        extends AbstractTestFramework
{
    private static AccioMDL accioMDL;

    public TestCumulativeMetric()
    {
        accioMDL = AccioMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        model("Orders",
                                "select * from main.orders",
                                List.of(
                                        column("orderkey", INTEGER, null, true),
                                        column("custkey", INTEGER, null, true),
                                        column("orderstatus", VARCHAR, null, true),
                                        column("totalprice", INTEGER, null, true),
                                        column("orderdate", DATE, null, true),
                                        column("orderpriority", VARCHAR, null, true),
                                        column("clerk", VARCHAR, null, true),
                                        column("shippriority", INTEGER, null, true),
                                        column("comment", VARCHAR, null, true)))))
                .setCumulativeMetrics(List.of(
                        cumulativeMetric("DailyRevenue",
                                "Orders", measure("totalprice", INTEGER, "sum", "totalprice"),
                                window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31")),
                        cumulativeMetric("WeeklyRevenue",
                                "Orders", measure("totalprice", INTEGER, "sum", "totalprice"),
                                window("orderdate", "orderdate", TimeUnit.WEEK, "1994-01-01", "1994-12-31")),
                        cumulativeMetric("MonthlyRevenue",
                                "Orders", measure("totalprice", INTEGER, "sum", "totalprice"),
                                window("orderdate", "orderdate", TimeUnit.MONTH, "1994-01-01", "1994-12-31")),
                        cumulativeMetric("QuarterlyRevenue",
                                "Orders", measure("totalprice", INTEGER, "sum", "totalprice"),
                                window("orderdate", "orderdate", TimeUnit.QUARTER, "1994-01-01", "1995-12-31")),
                        cumulativeMetric("YearlyRevenue",
                                "Orders", measure("totalprice", INTEGER, "sum", "totalprice"),
                                window("orderdate", "orderdate", TimeUnit.YEAR, "1994-01-01", "1998-12-31"))))
                .setDateSpine(new DateSpine(TimeUnit.DAY, "1970-01-01", "2077-12-31"))
                .build());
    }

    @Override
    protected void prepareData()
    {
        String orders = getClass().getClassLoader().getResource("tiny-orders.parquet").getPath();
        exec("create table orders as select * from '" + orders + "'");
    }

    @Test
    public void testCumulativeMetric()
    {
        assertThat(query(rewrite("select * from DailyRevenue")).size()).isEqualTo(365);
        assertThat(query(rewrite("select * from WeeklyRevenue")).size()).isEqualTo(53);
        assertThat(query(rewrite("select * from MonthlyRevenue")).size()).isEqualTo(12);
        assertThat(query(rewrite("select * from QuarterlyRevenue")).size()).isEqualTo(8);
        assertThat(query(rewrite("select * from YearlyRevenue")).size()).isEqualTo(5);
    }

    private String rewrite(String sql)
    {
        return rewrite(sql, accioMDL);
    }

    private String rewrite(String sql, AccioMDL accioMDL)
    {
        return AccioPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, accioMDL, List.of(METRIC_VIEW_SQL_REWRITE, ACCIO_SQL_REWRITE));
    }
}
