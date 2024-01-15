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

package io.accio.base.sqlrewrite;

import io.accio.base.AccioMDL;
import io.accio.base.AnalyzedMDL;
import io.accio.base.SessionContext;
import io.accio.base.dto.Model;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioTypes.DATE;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.View.view;
import static io.accio.base.sqlrewrite.AccioSqlRewrite.ACCIO_SQL_REWRITE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestView
        extends AbstractTestFramework
{
    private final Model orders;

    public TestView()
    {
        orders = model("Orders",
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
                        column("comment", VARCHAR, null, true),
                        column("lineitem", "Lineitem", "OrdersLineitem", true)),
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
        AccioMDL mdl = AccioMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setMetrics(List.of(
                        metric("CountOrders", "Orders",
                                List.of(column("custkey", INTEGER, null, true),
                                        column("orderstatus", DATE, null, true)),
                                List.of(column("count", INTEGER, null, true, "count(*)")),
                                List.of())))
                .setViews(List.of(
                        view("view1", "select * from CountOrders"),
                        view("view2", "select * from view1")))
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
        });
    }

    private String rewrite(String sql, AccioMDL accioMDL, boolean enableDynamicField)
    {
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog("accio")
                .setSchema("test")
                .setEnableDynamic(enableDynamicField)
                .build();
        return AccioPlanner.rewrite(sql, sessionContext, new AnalyzedMDL(accioMDL), List.of(ACCIO_SQL_REWRITE));
    }
}
