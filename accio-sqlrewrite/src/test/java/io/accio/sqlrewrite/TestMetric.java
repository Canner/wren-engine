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
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.testing.AbstractTestFramework;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioTypes.DATE;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.JoinType.MANY_TO_ONE;
import static io.accio.base.dto.JoinType.ONE_TO_MANY;
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
                                        column("customer", "Customer", "OrdersCustomer", true),
                                        column("lineitem", "Lineitem", "OrdersLineitem", true)),
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
                                "custkey"),
                        model("Lineitem",
                                "select * from main.lineitem",
                                List.of(
                                        column("orderkey", INTEGER, null, true),
                                        column("partkey", INTEGER, null, true),
                                        column("suppkey", INTEGER, null, true),
                                        column("linenumber", INTEGER, null, true),
                                        column("quantity", INTEGER, null, true),
                                        column("extendedprice", INTEGER, null, true),
                                        column("discount", INTEGER, null, true),
                                        column("tax", INTEGER, null, true),
                                        column("returnflag", VARCHAR, null, true),
                                        column("linestatus", VARCHAR, null, true),
                                        column("shipdate", DATE, null, true),
                                        column("commitdate", DATE, null, true),
                                        column("receiptdate", DATE, null, true),
                                        column("shipinstruct", VARCHAR, null, true),
                                        column("shipmode", VARCHAR, null, true),
                                        column("comment", VARCHAR, null, true),
                                        column("orderkey_linenumber", VARCHAR, null, true, "concat(orderkey, '-', linenumber)"),
                                        column("order_record", "Orders", "OrdersLineitem", true)),
                                "orderkey_linenumber")))
                .setRelationships(List.of(
                        relationship("OrdersCustomer", List.of("Orders", "Customer"), MANY_TO_ONE, "Orders.custkey = Customer.custkey"),
                        relationship("OrdersLineitem", List.of("Orders", "Lineitem"), ONE_TO_MANY, "Orders.orderkey = Lineitem.orderkey")))
                .setMetrics(List.of(
                        metric("TotalpriceByCustomer", "Customer",
                                List.of(
                                        column("name", VARCHAR, null, true),
                                        column("orderdate", DATE, null, true, "orders.orderdate")),
                                List.of(
                                        column("totalprice", INTEGER, null, true, "sum(orders.totalprice)")),
                                List.of(
                                        timeGrain("orderdata", "orderdate", List.of(DAY, MONTH, YEAR)))),
                        metric("TotalpriceByCustomerBaseOrders", "Orders",
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
                                List.of()),
                        metric("SumExtendedPriceByCustomer", "Customer",
                                List.of(column("name", VARCHAR, null, true),
                                        column("orderdate", DATE, null, true, "orders.orderdate")),
                                List.of(column("sum_extendprice", INTEGER, null, true, "sum(orders.lineitem.extendedprice)")),
                                List.of()),
                        metric("SumExtendedPriceByCustomerBaseLineitem", "Lineitem",
                                List.of(column("name", VARCHAR, null, true, "order_record.customer.name"),
                                        column("orderdate", DATE, null, true, "order_record.orderdate")),
                                List.of(column("sum_extendprice", INTEGER, null, true, "sum(extendedprice)")),
                                List.of()),
                        metric("RevenueByCustomer", "Customer",
                                List.of(column("name", VARCHAR, null, true),
                                        column("orderdate", DATE, null, true, "orders.orderdate")),
                                List.of(column("revenue", INTEGER, null, true, "sum(orders.lineitem.extendedprice * (1 - orders.lineitem.discount))")),
                                List.of()),
                        metric("RevenueByCustomerBaseLineitem", "Lineitem",
                                List.of(column("name", VARCHAR, null, true, "order_record.customer.name"),
                                        column("orderdate", DATE, null, true, "order_record.orderdate")),
                                List.of(column("revenue", INTEGER, null, true, "sum(extendedprice * (1 - discount))")),
                                List.of()),
                        metric("SumExtendedpriceAddTotalpriceByCustomerBaseOrders", "Orders",
                                List.of(column("name", VARCHAR, null, true, "customer.name"),
                                        column("orderdate", DATE, null, true, "orderdate")),
                                List.of(column("extAddTotalprice", INTEGER, null, true, "sum(totalprice + lineitem.extendedprice)")),
                                List.of()),
                        metric("SumExtendedpriceAddTotalpriceByCustomerBaseLineitem", "Lineitem",
                                List.of(column("name", VARCHAR, null, true, "order_record.customer.name"),
                                        column("orderdate", DATE, null, true, "order_record.orderdate")),
                                List.of(column("extAddTotalprice", INTEGER, null, true, "sum(order_record.totalprice + extendedprice)")),
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
        String lineitem = getClass().getClassLoader().getResource("tiny-lineitem.parquet").getPath();
        exec("create table lineitem as select * from '" + lineitem + "'");
    }

    @Test
    public void testMetricUseToOneRelationship()
    {
        List<List<Object>> result = query(rewrite("select * from TotalpriceByCustomerBaseOrders"));
        assertThat(result.get(0).size()).isEqualTo(3);
        assertThat(result.size()).isEqualTo(14958);

        assertThatNoException()
                .isThrownBy(() -> query(rewrite("select name, orderdate, totalprice from TotalpriceByCustomerBaseOrders")));

        List<List<Object>> measureRelationship = query(rewrite("select * from NumberCustomerByDate"));
        assertThat(measureRelationship.get(0).size()).isEqualTo(2);
        assertThat(measureRelationship.size()).isEqualTo(2401);
    }

    @Test
    public void testMetricUseToManyRelationship()
    {
        List<List<Object>> result = query(rewrite("select * from TotalpriceByCustomer"));
        assertThat(result.get(0).size()).isEqualTo(3);
        assertThat(result.size()).isEqualTo(15458);

        assertThat(query(rewrite("SELECT totalprice FROM TotalpriceByCustomer " +
                "WHERE \"name\" = 'Customer#000000392' and orderdate = DATE '1996-01-10'")).get(0).get(0))
                .isEqualTo(query(rewrite("SELECT totalprice FROM TotalpriceByCustomerBaseOrders " +
                        "WHERE \"name\" = 'Customer#000000392' and orderdate = DATE '1996-01-10'")).get(0).get(0));
    }

    @Test
    public void testMetricUseThreeLevelRelationship()
    {
        List<List<Object>> result = query(rewrite("select * from SumExtendedPriceByCustomer"));
        assertThat(result.get(0).size()).isEqualTo(3);
        assertThat(result.size()).isEqualTo(15458);

        assertThat(query(rewrite("SELECT sum_extendprice FROM SumExtendedPriceByCustomer " +
                "WHERE \"name\" = 'Customer#000000392' and orderdate = DATE '1996-01-10'")).get(0).get(0))
                .isEqualTo(query(rewrite("SELECT sum_extendprice FROM SumExtendedPriceByCustomerBaseLineitem " +
                        "WHERE \"name\" = 'Customer#000000392' and orderdate = DATE '1996-01-10'")).get(0).get(0));
    }

    @Test
    public void testMultipleRelationshipFieldInExpression()
    {
        List<List<Object>> result = query(rewrite("select * from RevenueByCustomer"));
        assertThat(result.get(0).size()).isEqualTo(3);
        assertThat(result.size()).isEqualTo(15458);

        assertThat(query(rewrite("SELECT revenue FROM RevenueByCustomer " +
                "WHERE \"name\" = 'Customer#000000392' and orderdate = DATE '1996-01-10'")).get(0).get(0))
                .isEqualTo(query(rewrite("SELECT revenue FROM RevenueByCustomerBaseLineitem " +
                        "WHERE \"name\" = 'Customer#000000392' and orderdate = DATE '1996-01-10'")).get(0).get(0));
    }

    @Test
    public void testExpressionIncludeRelationshipAndNonRelatinonship()
    {
        assertThat(query(rewrite("SELECT extAddTotalprice FROM SumExtendedpriceAddTotalpriceByCustomerBaseOrders " +
                "WHERE \"name\" = 'Customer#000000392' and orderdate = DATE '1996-01-10'")).get(0).get(0))
                .isEqualTo(query(rewrite("SELECT extAddTotalprice FROM SumExtendedpriceAddTotalpriceByCustomerBaseLineitem " +
                        "WHERE \"name\" = 'Customer#000000392' and orderdate = DATE '1996-01-10'")).get(0).get(0));
    }

    @Test
    public void testModelOnMetric()
    {
        List<Model> models = ImmutableList.<Model>builder()
                .addAll(manifest.getModels())
                .add(onBaseObject(
                        "testModelOnMetric",
                        "TotalpriceByCustomerBaseOrders",
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

    @Test
    public void testMetricOnMetric()
    {
        List<Metric> metrics = ImmutableList.<Metric>builder()
                .addAll(manifest.getMetrics())
                .add(metric(
                        "testMetricOnMetric",
                        "TotalpriceByCustomerBaseOrders",
                        List.of(column("orderyear", VARCHAR, null, true, "DATE_TRUNC('YEAR', orderdate)")),
                        List.of(column("revenue", INTEGER, null, true, "sum(totalprice)")),
                        List.of()))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(copyOf(manifest).setMetrics(metrics).build());

        List<List<Object>> result = query(rewrite("SELECT * FROM testMetricOnMetric ORDER BY orderyear", mdl));
        assertThat(result.get(0).size()).isEqualTo(2);
        assertThat(result.size()).isEqualTo(7);
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
