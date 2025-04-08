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

import io.wren.base.AnalyzedMDL;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.View;
import org.testng.annotations.Test;

import java.util.List;

import static io.wren.base.sqlrewrite.GenerateViewRewrite.GENERATE_VIEW_REWRITE;
import static io.wren.base.sqlrewrite.WrenSqlRewrite.WREN_SQL_REWRITE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestView
        extends AbstractTestFramework
{
    private final Model orders;

    public TestView()
    {
        orders = Model.model("Orders",
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
                        Column.column("comment", WrenTypes.VARCHAR, null, true),
                        Column.column("lineitem", "Lineitem", "OrdersLineitem", true)),
                "orderkey");
    }

    @Override
    protected void prepareData()
    {
        String orders = requireNonNull(getClass().getClassLoader().getResource("tiny-orders.parquet")).getPath();
        exec("create table orders as select * from '" + orders + "'");
    }

    @Test
    public void testView()
    {
        WrenMDL mdl = WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setMetrics(List.of(
                        Metric.metric("CountOrders", "Orders",
                                List.of(Column.column("custkey", WrenTypes.INTEGER, null, true),
                                        Column.column("orderstatus", WrenTypes.DATE, null, true)),
                                List.of(Column.column("count", WrenTypes.INTEGER, null, true, "count(*)")),
                                List.of())))
                .setViews(List.of(
                        View.view("view1", "select * from CountOrders"),
                        View.view("view2", "select * from view1"),
                        View.view("oneDimCount", "select custkey from CountOrders"),
                        View.view("oneDimCount2", "select * from oneDimCount"),
                        View.view("cteInView", "with cte as (select * from CountOrders) select * from cte")))
                .build());

        List.of(true, false).forEach(enableDynamicFields -> {
            assertThat(query(rewrite("SELECT custkey, count FROM view1 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey, count FROM CountOrders WHERE custkey = 370", mdl, false)));
            assertThat(query(rewrite("SELECT custkey, count FROM view2 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey, count FROM CountOrders WHERE custkey = 370", mdl, false)));

            assertThat(query(rewrite("SELECT * FROM view1 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT * FROM CountOrders WHERE custkey = 370", mdl, false)));
            assertThat(query(rewrite("SELECT * FROM view2 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT * FROM CountOrders WHERE custkey = 370", mdl, false)));

            assertThat(query(rewrite("SELECT * FROM oneDimCount WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey FROM CountOrders WHERE custkey = 370", mdl, enableDynamicFields)));
            assertThat(query(rewrite("SELECT custkey FROM oneDimCount WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey FROM CountOrders WHERE custkey = 370", mdl, enableDynamicFields)));

            assertThat(query(rewrite("SELECT * FROM oneDimCount2 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey FROM CountOrders WHERE custkey = 370", mdl, enableDynamicFields)));
            assertThat(query(rewrite("SELECT custkey FROM oneDimCount2 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey FROM CountOrders WHERE custkey = 370", mdl, enableDynamicFields)));

            assertThat(query(rewrite("SELECT custkey, count FROM cteInView WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey, count FROM CountOrders WHERE custkey = 370", mdl, false)));
        });
    }

    @Test
    public void testViewWithoutOrder()
    {
        WrenMDL mdl = WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setViews(List.of(
                        View.view("view2", "select * from view1"),
                        View.view("view3", "select * from view1"),
                        View.view("view4", "select * from view2"),
                        View.view("view1", "select * from Orders")))
                .build());
        List.of(true, false).forEach(enableDynamicFields -> {
            assertThat(query(rewrite("SELECT custkey FROM view1 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey FROM Orders WHERE custkey = 370", mdl, false)));
            assertThat(query(rewrite("SELECT custkey FROM view2 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey FROM Orders WHERE custkey = 370", mdl, false)));
            assertThat(query(rewrite("SELECT custkey FROM view3 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey FROM Orders WHERE custkey = 370", mdl, false)));
            assertThat(query(rewrite("SELECT custkey FROM view4 WHERE custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey FROM Orders WHERE custkey = 370", mdl, false)));
            assertThat(query(rewrite("SELECT v2.custkey FROM view2 v2 JOIN view4 v4 ON v2.orderkey = v4.orderkey WHERE v2.custkey = 370", mdl, enableDynamicFields)))
                    .isEqualTo(query(rewrite("SELECT custkey FROM Orders WHERE custkey = 370", mdl, false)));
        });
    }

    @Test
    public void testCycleDependencyView()
    {
        WrenMDL mdl = WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setViews(List.of(
                        View.view("view2", "select * from view1"),
                        View.view("view1", "select * from view2"),
                        View.view("view3", "select * from view3")))
                .build());

        assertThatThrownBy(() -> query(rewrite("select * from view1", mdl, false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("found cycle in view");

        assertThatThrownBy(() -> query(rewrite("select * from view3", mdl, false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("found issue in view")
                .getCause()
                .hasMessageContaining("loops not allowed");
    }

    @Test
    public void testCustomCTE()
    {
        WrenMDL mdl = WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setViews(List.of(
                        View.view("view1", "select * from orders"),
                        View.view("view2", "with cte as (select * from view1) select * from cte")))
                .build());
        assertThat(query(rewrite("WITH cte AS (SELECT * FROM view1) SELECT * FROM cte", mdl, false)))
                .isEqualTo(query(rewrite("SELECT * FROM orders", mdl, false)));
        assertThat(query(rewrite("WITH cte AS (SELECT * FROM view1) SELECT * FROM cte", mdl, true)))
                .isEqualTo(query(rewrite("SELECT * FROM orders", mdl, false)));
        assertThat(query(rewrite("WITH cte as (SELECT * FROM view2) SELECT * FROM cte", mdl, false)))
                .isEqualTo(query(rewrite("SELECT * FROM orders", mdl, false)));
        assertThat(query(rewrite("WITH cte as (SELECT * FROM view2) SELECT * FROM cte", mdl, true)))
                .isEqualTo(query(rewrite("SELECT * FROM orders", mdl, false)));
    }

    private String rewrite(String sql, WrenMDL wrenMDL, boolean enableDynamicField)
    {
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog("wren")
                .setSchema("test")
                .setEnableDynamic(enableDynamicField)
                .build();
        return WrenPlanner.rewrite(sql, sessionContext, new AnalyzedMDL(wrenMDL, null), List.of(GENERATE_VIEW_REWRITE, WREN_SQL_REWRITE));
    }
}
