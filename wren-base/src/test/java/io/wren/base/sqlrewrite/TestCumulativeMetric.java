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

package io.wren.base.sqlrewrite;

import com.google.common.collect.ImmutableList;
import io.wren.base.AnalyzedMDL;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.CumulativeMetric;
import io.wren.base.dto.DateSpine;
import io.wren.base.dto.Manifest;
import io.wren.base.dto.Measure;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.TimeUnit;
import io.wren.base.dto.Window;
import org.testng.annotations.Test;

import java.util.List;

import static io.wren.base.sqlrewrite.WrenSqlRewrite.WREN_SQL_REWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCumulativeMetric
        extends AbstractTestFramework
{
    private final Manifest manifest;
    private final WrenMDL wrenMDL;

    public TestCumulativeMetric()
    {
        manifest = withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("Orders",
                                "select * from main.orders",
                                List.of(
                                        Column.column("orderkey", WrenTypes.INTEGER, null, true),
                                        Column.column("custkey", WrenTypes.INTEGER, null, true),
                                        Column.column("orderstatus", WrenTypes.VARCHAR, null, true),
                                        Column.column("totalprice", WrenTypes.INTEGER, null, true),
                                        Column.column("orderdate", WrenTypes.DATE, null, true),
                                        Column.column("orderpriority", WrenTypes.VARCHAR, null, true),
                                        Column.column("clerk", WrenTypes.VARCHAR, null, true),
                                        Column.column("shippriority", WrenTypes.INTEGER, null, true),
                                        Column.column("comment", WrenTypes.VARCHAR, null, true)))))
                .setCumulativeMetrics(List.of(
                        CumulativeMetric.cumulativeMetric("DailyRevenue",
                                "Orders", Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                                Window.window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31")),
                        CumulativeMetric.cumulativeMetric("WeeklyRevenue",
                                "Orders", Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                                Window.window("orderdate", "orderdate", TimeUnit.WEEK, "1994-01-01", "1994-12-31")),
                        CumulativeMetric.cumulativeMetric("MonthlyRevenue",
                                "Orders", Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                                Window.window("orderdate", "orderdate", TimeUnit.MONTH, "1994-01-01", "1994-12-31")),
                        CumulativeMetric.cumulativeMetric("QuarterlyRevenue",
                                "Orders", Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                                Window.window("orderdate", "orderdate", TimeUnit.QUARTER, "1994-01-01", "1995-12-31")),
                        CumulativeMetric.cumulativeMetric("YearlyRevenue",
                                "Orders", Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                                Window.window("orderdate", "orderdate", TimeUnit.YEAR, "1994-01-01", "1998-12-31"))))
                .setDateSpine(new DateSpine(TimeUnit.DAY, "1970-01-01", "2077-12-31"))
                .build();
        wrenMDL = WrenMDL.fromManifest(manifest);
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
        List.of(true, false).forEach(enableDynamic -> {
            assertThat(query(rewrite("select * from DailyRevenue", wrenMDL, enableDynamic)).size()).isEqualTo(365);
            assertThat(query(rewrite("select * from WeeklyRevenue", wrenMDL, enableDynamic)).size()).isEqualTo(53);
            assertThat(query(rewrite("select * from MonthlyRevenue", wrenMDL, enableDynamic)).size()).isEqualTo(12);
            assertThat(query(rewrite("select * from QuarterlyRevenue", wrenMDL, enableDynamic)).size()).isEqualTo(8);
            assertThat(query(rewrite("select * from YearlyRevenue", wrenMDL, enableDynamic)).size()).isEqualTo(5);
        });

        List.of(true, false).forEach(enableDynamic -> {
            assertThatCode(() -> query(rewrite("SELECT 1 FROM DailyRevenue", wrenMDL, true)))
                    .doesNotThrowAnyException();
        });
    }

    @Test
    public void testModelOnCumulativeMetric()
    {
        List<Model> models = ImmutableList.<Model>builder()
                .addAll(manifest.getModels())
                .add(Model.onBaseObject(
                        "testModelOnCumulativeMetric",
                        "WeeklyRevenue",
                        List.of(
                                Column.column("totalprice", WrenTypes.INTEGER, null, false),
                                Column.column("orderdate", "DATE", null, false)),
                        "orderdate"))
                .build();
        WrenMDL mdl = WrenMDL.fromManifest(
                copyOf(manifest)
                        .setModels(models)
                        .build());

        List.of(true, false).forEach(enableDynamic -> {
            List<List<Object>> result = query(rewrite("select * from testModelOnCumulativeMetric", mdl, enableDynamic));
            assertThat(result.get(0).size()).isEqualTo(2);
            assertThat(result.size()).isEqualTo(53);
        });

        List.of(true, false).forEach(enableDynamic -> {
            assertThatCode(() -> query(rewrite("SELECT 1 FROM testModelOnCumulativeMetric", mdl, true)))
                    .doesNotThrowAnyException();
        });
    }

    @Test
    public void testMetricOnCumulativeMetric()
    {
        List<Metric> metrics = ImmutableList.<Metric>builder()
                .addAll(manifest.getMetrics())
                .add(Metric.metric(
                        "testMetricOnCumulativeMetric",
                        "DailyRevenue",
                        List.of(Column.column("ordermonth", "DATE", null, false, "date_trunc('month', orderdate)")),
                        List.of(Column.column("totalprice", WrenTypes.INTEGER, null, false, "sum(totalprice)")),
                        List.of()))
                .build();
        WrenMDL mdl = WrenMDL.fromManifest(
                copyOf(manifest)
                        .setMetrics(metrics)
                        .build());

        List.of(true, false).forEach(enableDynamic -> {
            List<List<Object>> result = query(rewrite("SELECT * FROM testMetricOnCumulativeMetric ORDER BY ordermonth", mdl, enableDynamic));
            assertThat(result.get(0).size()).isEqualTo(2);
            assertThat(result.size()).isEqualTo(12);
        });

        List.of(true, false).forEach(enableDynamic -> {
            assertThatCode(() -> query(rewrite("SELECT 1 FROM testMetricOnCumulativeMetric", mdl, true)))
                    .doesNotThrowAnyException();
        });
    }

    @Test
    public void testCumulativeMetricOnCumulativeMetric()
    {
        List<CumulativeMetric> cumulativeMetrics = ImmutableList.<CumulativeMetric>builder()
                .addAll(manifest.getCumulativeMetrics())
                .add(CumulativeMetric.cumulativeMetric("testCumulativeMetricOnCumulativeMetric",
                        "YearlyRevenue", Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                        Window.window("orderyear", "orderdate", TimeUnit.YEAR, "1994-01-01", "1998-12-31")))
                .build();
        WrenMDL mdl = WrenMDL.fromManifest(
                copyOf(manifest)
                        .setCumulativeMetrics(cumulativeMetrics)
                        .build());

        List.of(true, false).forEach(enableDynamic -> {
            List<List<Object>> result = query(rewrite("SELECT * FROM testCumulativeMetricOnCumulativeMetric ORDER BY orderyear", mdl, enableDynamic));
            assertThat(result.get(0).size()).isEqualTo(2);
            assertThat(result.size()).isEqualTo(5);
        });

        List.of(true, false).forEach(enableDynamic -> {
            assertThatCode(() -> query(rewrite("SELECT 1 FROM testCumulativeMetricOnCumulativeMetric", mdl, true)))
                    .doesNotThrowAnyException();
        });
    }

    @Test
    public void testInvalidCumulativeMetricOnCumulativeMetric()
    {
        List<CumulativeMetric> cumulativeMetrics = ImmutableList.<CumulativeMetric>builder()
                .addAll(manifest.getCumulativeMetrics())
                .add(CumulativeMetric.cumulativeMetric("testInvalidCumulativeMetricOnCumulativeMetric",
                        "YearlyRevenue", Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                        // window refColumn is a measure that belongs to cumulative metric
                        Window.window("foo", "totalprice", TimeUnit.YEAR, "1994-01-01", "1998-12-31")))
                .build();
        WrenMDL mdl = WrenMDL.fromManifest(
                copyOf(manifest)
                        .setCumulativeMetrics(cumulativeMetrics)
                        .build());

        List.of(true, false).forEach(enableDynamic -> {
            assertThatThrownBy(() -> rewrite("SELECT * FROM testInvalidCumulativeMetricOnCumulativeMetric", mdl, enableDynamic))
                    .hasMessage("CumulativeMetric measure cannot be window as it is not date/timestamp type");
        });
    }

    @Test
    public void testCumulativeMetricOnMetric()
    {
        List<Metric> metrics = ImmutableList.of(
                Metric.metric("RevenueByOrderdate", "Orders",
                        List.of(Column.column("orderdate", WrenTypes.DATE, null, true, "orderdate")),
                        List.of(Column.column("totalprice", WrenTypes.INTEGER, null, true, "sum(totalprice)")),
                        List.of()));
        List<CumulativeMetric> cumulativeMetrics = ImmutableList.<CumulativeMetric>builder()
                .addAll(manifest.getCumulativeMetrics())
                .add(CumulativeMetric.cumulativeMetric("testCumulativeMetricOnMetric",
                        "RevenueByOrderdate", Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                        Window.window("orderyear", "orderdate", TimeUnit.YEAR, "1994-01-01", "1998-12-31")))
                .build();
        WrenMDL mdl = WrenMDL.fromManifest(
                copyOf(manifest)
                        .setMetrics(metrics)
                        .setCumulativeMetrics(cumulativeMetrics)
                        .build());

        List.of(true, false).forEach(enableDynamic -> {
            List<List<Object>> result = query(rewrite("SELECT * FROM testCumulativeMetricOnMetric ORDER BY orderyear", mdl, enableDynamic));
            assertThat(result.get(0).size()).isEqualTo(2);
            assertThat(result.size()).isEqualTo(5);
        });

        List.of(true, false).forEach(enableDynamic -> {
            assertThatCode(() -> query(rewrite("SELECT 1 FROM testCumulativeMetricOnMetric", mdl, true)))
                    .doesNotThrowAnyException();
        });
    }

    private String rewrite(String sql, WrenMDL wrenMDL, boolean enableDynamic)
    {
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog("wren")
                .setSchema("test")
                .setEnableDynamic(enableDynamic)
                .build();
        return WrenPlanner.rewrite(sql, sessionContext, new AnalyzedMDL(wrenMDL, null), List.of(WREN_SQL_REWRITE));
    }
}
