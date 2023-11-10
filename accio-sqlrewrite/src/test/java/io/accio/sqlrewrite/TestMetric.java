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

import com.google.common.collect.ImmutableList;
import io.accio.base.AccioMDL;
import io.accio.base.dto.Manifest;
import io.accio.base.dto.Model;
import io.accio.testing.AbstractTestFramework;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioTypes.DATE;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.JoinType.MANY_TO_ONE;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Model.onBaseObject;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.base.dto.TimeGrain.timeGrain;
import static io.accio.base.dto.TimeUnit.DAY;
import static io.accio.base.dto.TimeUnit.MONTH;
import static io.accio.base.dto.TimeUnit.YEAR;
import static io.accio.sqlrewrite.AccioSqlRewrite.ACCIO_SQL_REWRITE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

public class TestMetric
        extends AbstractTestFramework
{
    private final Manifest manifest;
    private final AccioMDL accioMDL;

    public TestMetric()
    {
        manifest = withDefaultCatalogSchema()
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
                                        column("comment", VARCHAR, null, true),
                                        column("customer_name", VARCHAR, null, true, "customer.name"),
                                        column("cumstomer_address", VARCHAR, null, true, "customer.address"),
                                        column("customer", "Customer", "OrdersCustomer", true)),
                                "orderkey"),
                        model("Customer",
                                "select * from main.customer",
                                List.of(
                                        column("custkey", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        column("address", VARCHAR, null, true),
                                        column("nationkey", INTEGER, null, true),
                                        column("phone", VARCHAR, null, true),
                                        column("acctbal", INTEGER, null, true),
                                        column("mktsegment", VARCHAR, null, true),
                                        column("comment", VARCHAR, null, true),
                                        column("orders", "Orders", "OrdersCustomer", true)),
                                "custkey")))
                .setRelationships(List.of(
                        relationship("OrdersCustomer", List.of("Orders", "Customer"), MANY_TO_ONE, "Orders.custkey = Customer.custkey")))
                .setMetrics(List.of(
                        metric("RevenueByCustomer", "Customer",
                                List.of(
                                        column("name", VARCHAR, null, true),
                                        column("orderdate", DATE, null, true, "orders.orderdate")),
                                List.of(
                                        column("totalprice", INTEGER, null, true, "sum(orders.totalprice)")),
                                List.of(
                                        timeGrain("orderdata", "orderdate", List.of(DAY, MONTH, YEAR)))),
                        metric("RevenueByCustomerBaseOrders", "Orders",
                                List.of(
                                        column("name", VARCHAR, null, true, "customer.name"),
                                        column("orderdate", DATE, null, true)),
                                List.of(
                                        column("totalprice", INTEGER, null, true, "sum(totalprice)")),
                                List.of(
                                        timeGrain("orderdata", "orderdate", List.of(DAY, MONTH, YEAR)))),
                        metric("NumberCustomerByDate", "Orders",
                                List.of(column("orderdate", DATE, null, true)),
                                List.of(column("count_of_customer", INTEGER, null, true, "count(distinct customer.name)")),
                                List.of())))
                .build();
        accioMDL = AccioMDL.fromManifest(manifest);
    }

    @Override
    protected void prepareData()
    {
        String orders = getClass().getClassLoader().getResource("tiny-orders.parquet").getPath();
        exec("create table orders as select * from '" + orders + "'");
        String customer = getClass().getClassLoader().getResource("tiny-customer.parquet").getPath();
        exec("create table customer as select * from '" + customer + "'");
    }

    @Test
    public void testMetricUseToOneRelationship()
    {
        List<List<Object>> result = query(rewrite("select * from RevenueByCustomerBaseOrders"));
        assertThat(result.get(0).size()).isEqualTo(3);
        assertThat(result.size()).isEqualTo(14958);

        assertThatNoException()
                .isThrownBy(() -> query(rewrite("select name, orderdate, totalprice from RevenueByCustomerBaseOrders")));

        List<List<Object>> measureRelationship = query(rewrite("select * from NumberCustomerByDate"));
        assertThat(measureRelationship.get(0).size()).isEqualTo(2);
        assertThat(measureRelationship.size()).isEqualTo(2401);
    }

    @Test
    public void testModelOnMetric()
    {
        List<Model> models = ImmutableList.<Model>builder()
                .addAll(manifest.getModels())
                .add(onBaseObject(
                        "testModelOnMetric",
                        "RevenueByCustomerBaseOrders",
                        List.of(
                                column("name", VARCHAR, null, true),
                                column("revenue", INTEGER, null, true, "totalprice")),
                        "name"))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(
                copyOf(manifest)
                        .setModels(models)
                        .build());

        List<List<Object>> result = query(rewrite("select * from testModelOnMetric", mdl));
        assertThat(result.get(0).size()).isEqualTo(2);
        assertThat(result.size()).isEqualTo(14958);
    }

    private String rewrite(String sql)
    {
        return rewrite(sql, accioMDL);
    }

    private String rewrite(String sql, AccioMDL accioMDL)
    {
        return AccioPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, accioMDL, List.of(ACCIO_SQL_REWRITE));
    }
}
