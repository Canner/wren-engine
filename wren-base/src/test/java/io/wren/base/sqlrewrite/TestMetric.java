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
import io.wren.base.dto.JoinType;
import io.wren.base.dto.Manifest;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationship;
import io.wren.base.dto.TimeGrain;
import io.wren.base.dto.TimeUnit;
import org.testng.annotations.Test;

import java.util.List;

import static io.wren.base.dto.TableReference.tableReference;
import static io.wren.base.sqlrewrite.WrenSqlRewrite.WREN_SQL_REWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMetric
        extends AbstractTestFramework
{
    private final Manifest manifest;
    private final WrenMDL wrenMDL;

    public TestMetric()
    {
        manifest = withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.onTableReference("Orders",
                                tableReference("memory", "main", "orders"),
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
                                        Column.calculatedColumn("customer_name", WrenTypes.VARCHAR, "customer.name"),
                                        Column.calculatedColumn("cumstomer_address", WrenTypes.VARCHAR, "customer.address"),
                                        Column.column("customer", "Customer", "OrdersCustomer", true),
                                        Column.column("lineitem", "Lineitem", "OrdersLineitem", true)),
                                "orderkey"),
                        Model.model("Customer",
                                "select * from main.customer",
                                List.of(
                                        Column.column("custkey", WrenTypes.INTEGER, null, true),
                                        Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.column("address", WrenTypes.VARCHAR, null, true),
                                        Column.column("nationkey", WrenTypes.INTEGER, null, true),
                                        Column.column("phone", WrenTypes.VARCHAR, null, true),
                                        Column.column("acctbal", WrenTypes.INTEGER, null, true),
                                        Column.column("mktsegment", WrenTypes.VARCHAR, null, true),
                                        Column.column("comment", WrenTypes.VARCHAR, null, true),
                                        Column.column("orders", "Orders", "OrdersCustomer", true)),
                                "custkey"),
                        Model.model("Lineitem",
                                "select * from main.lineitem",
                                List.of(
                                        Column.column("orderkey", WrenTypes.INTEGER, null, true),
                                        Column.column("partkey", WrenTypes.INTEGER, null, true),
                                        Column.column("suppkey", WrenTypes.INTEGER, null, true),
                                        Column.column("linenumber", WrenTypes.INTEGER, null, true),
                                        Column.column("quantity", WrenTypes.INTEGER, null, true),
                                        Column.column("extendedprice", WrenTypes.INTEGER, null, true),
                                        Column.column("discount", WrenTypes.INTEGER, null, true),
                                        Column.column("tax", WrenTypes.INTEGER, null, true),
                                        Column.column("returnflag", WrenTypes.VARCHAR, null, true),
                                        Column.column("linestatus", WrenTypes.VARCHAR, null, true),
                                        Column.column("shipdate", WrenTypes.DATE, null, true),
                                        Column.column("commitdate", WrenTypes.DATE, null, true),
                                        Column.column("receiptdate", WrenTypes.DATE, null, true),
                                        Column.column("shipinstruct", WrenTypes.VARCHAR, null, true),
                                        Column.column("shipmode", WrenTypes.VARCHAR, null, true),
                                        Column.column("comment", WrenTypes.VARCHAR, null, true),
                                        Column.column("orderkey_linenumber", WrenTypes.VARCHAR, null, true, "concat(orderkey, '-', linenumber)"),
                                        Column.column("order_record", "Orders", "OrdersLineitem", true)),
                                "orderkey_linenumber")))
                .setRelationships(List.of(
                        Relationship.relationship("OrdersCustomer", List.of("Orders", "Customer"), JoinType.MANY_TO_ONE, "Orders.custkey = Customer.custkey"),
                        Relationship.relationship("OrdersLineitem", List.of("Orders", "Lineitem"), JoinType.ONE_TO_MANY, "Orders.orderkey = Lineitem.orderkey")))
                .setMetrics(List.of(
                        Metric.metric("TotalpriceByCustomer", "Customer",
                                List.of(
                                        Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.column("orderdate", WrenTypes.DATE, null, true, "orders.orderdate")),
                                List.of(
                                        Column.column("totalprice", WrenTypes.INTEGER, null, true, "sum(orders.totalprice)")),
                                List.of(
                                        TimeGrain.timeGrain("orderdata", "orderdate", List.of(TimeUnit.DAY, TimeUnit.MONTH, TimeUnit.YEAR)))),
                        Metric.metric("TotalpriceByCustomerYear", "Customer",
                                List.of(
                                        Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.column("orderdate", WrenTypes.DATE, null, true, "date_trunc('YEAR', orders.orderdate)")),
                                List.of(
                                        Column.column("totalprice", WrenTypes.INTEGER, null, true, "sum(orders.totalprice)")),
                                List.of(
                                        TimeGrain.timeGrain("orderdata", "orderdate", List.of(TimeUnit.DAY, TimeUnit.MONTH, TimeUnit.YEAR)))),
                        Metric.metric("TotalpriceByCustomerBaseOrders", "Orders",
                                List.of(
                                        Column.column("name", WrenTypes.VARCHAR, null, true, "customer.name"),
                                        Column.column("orderdate", WrenTypes.DATE, null, true)),
                                List.of(
                                        Column.column("totalprice", WrenTypes.INTEGER, null, true, "sum(totalprice)")),
                                List.of(
                                        TimeGrain.timeGrain("orderdata", "orderdate", List.of(TimeUnit.DAY, TimeUnit.MONTH, TimeUnit.YEAR)))),
                        Metric.metric("TotalpriceByCustomerBaseOrdersYear", "Orders",
                                List.of(
                                        Column.column("name", WrenTypes.VARCHAR, null, true, "customer.name"),
                                        Column.column("orderdate", WrenTypes.DATE, null, true, "date_trunc('YEAR', orderdate)")),
                                List.of(
                                        Column.column("totalprice", WrenTypes.INTEGER, null, true, "sum(totalprice)")),
                                List.of()),
                        Metric.metric("NumberCustomerByDate", "Orders",
                                List.of(Column.column("orderdate", WrenTypes.DATE, null, true)),
                                List.of(Column.column("count_of_customer", WrenTypes.INTEGER, null, true, "count(distinct customer.name)")),
                                List.of()),
                        Metric.metric("SumExtendedPriceByCustomer", "Customer",
                                List.of(Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.column("orderdate", WrenTypes.DATE, null, true, "orders.orderdate")),
                                List.of(Column.column("sum_extendprice", WrenTypes.INTEGER, null, true, "sum(orders.lineitem.extendedprice)")),
                                List.of()),
                        Metric.metric("SumExtendedPriceByCustomerBaseLineitem", "Lineitem",
                                List.of(Column.column("name", WrenTypes.VARCHAR, null, true, "order_record.customer.name"),
                                        Column.column("orderdate", WrenTypes.DATE, null, true, "order_record.orderdate")),
                                List.of(Column.column("sum_extendprice", WrenTypes.INTEGER, null, true, "sum(extendedprice)")),
                                List.of()),
                        Metric.metric("RevenueByCustomer", "Customer",
                                List.of(Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.column("orderdate", WrenTypes.DATE, null, true, "orders.orderdate")),
                                List.of(Column.column("revenue", WrenTypes.INTEGER, null, true, "sum(orders.lineitem.extendedprice * (1 - orders.lineitem.discount))")),
                                List.of()),
                        Metric.metric("RevenueByCustomerBaseLineitem", "Lineitem",
                                List.of(Column.column("name", WrenTypes.VARCHAR, null, true, "order_record.customer.name"),
                                        Column.column("orderdate", WrenTypes.DATE, null, true, "order_record.orderdate")),
                                List.of(Column.column("revenue", WrenTypes.INTEGER, null, true, "sum(extendedprice * (1 - discount))")),
                                List.of()),
                        Metric.metric("SumExtendedpriceAddTotalpriceByCustomerBaseOrders", "Orders",
                                List.of(Column.column("name", WrenTypes.VARCHAR, null, true, "customer.name"),
                                        Column.column("orderdate", WrenTypes.DATE, null, true, "orderdate")),
                                List.of(Column.column("extAddTotalprice", WrenTypes.INTEGER, null, true, "sum(totalprice + lineitem.extendedprice)")),
                                List.of()),
                        Metric.metric("SumExtendedpriceAddTotalpriceByCustomerBaseLineitem", "Lineitem",
                                List.of(Column.column("name", WrenTypes.VARCHAR, null, true, "order_record.customer.name"),
                                        Column.column("orderdate", WrenTypes.DATE, null, true, "order_record.orderdate")),
                                List.of(Column.column("extAddTotalprice", WrenTypes.INTEGER, null, true, "sum(order_record.totalprice + extendedprice)")),
                                List.of()),
                        Metric.metric("CountOrderkey", "Orders",
                                List.of(Column.column("orderkey", WrenTypes.INTEGER, null, true),
                                        Column.column("orderdate", WrenTypes.DATE, null, true)),
                                List.of(Column.column("count", WrenTypes.INTEGER, null, true, "count(orderkey)")),
                                List.of()),
                        Metric.metric("CountOrders", "Orders",
                                List.of(Column.column("custkey", WrenTypes.INTEGER, null, true),
                                        Column.column("orderstatus", WrenTypes.VARCHAR, null, true)),
                                List.of(Column.column("count", WrenTypes.INTEGER, null, true, "count(*)")),
                                List.of()),
                        Metric.metric("CountOrders2", "CountOrders",
                                List.of(Column.column("orderstatus", WrenTypes.VARCHAR, null, true)),
                                List.of(Column.column("sum_count", WrenTypes.INTEGER, null, true, "sum(count)")),
                                List.of())))
                .build();
        wrenMDL = WrenMDL.fromManifest(manifest);
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
        List<List<Object>> result = query(rewrite("select * from TotalpriceByCustomerBaseOrders", true));
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
    public void testSelectEmptyWithDynamic()
    {
        assertThatCode(() -> query(rewrite("select 1 from TotalpriceByCustomerBaseOrders", true)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testModelOnMetric()
    {
        List<Model> models = ImmutableList.<Model>builder()
                .addAll(manifest.getModels())
                .add(Model.onBaseObject(
                        "testModelOnMetric",
                        "TotalpriceByCustomerBaseOrders",
                        List.of(
                                Column.column("name", WrenTypes.VARCHAR, null, true),
                                Column.column("revenue", WrenTypes.INTEGER, null, true, "totalprice")),
                        "name"))
                .build();
        WrenMDL mdl = WrenMDL.fromManifest(
                copyOf(manifest)
                        .setModels(models)
                        .build());

        List<List<Object>> result = query(rewrite("select * from testModelOnMetric", mdl));
        assertThat(result.get(0).size()).isEqualTo(2);
        assertThat(result.size()).isEqualTo(14958);

        assertThatCode(() -> query(rewrite("select 1 from testModelOnMetric", mdl, true)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testMetricOnMetric()
    {
        List<Metric> metrics = ImmutableList.<Metric>builder()
                .addAll(manifest.getMetrics())
                .add(Metric.metric(
                        "testMetricOnMetric",
                        "TotalpriceByCustomerBaseOrders",
                        List.of(Column.column("orderyear", WrenTypes.VARCHAR, null, true, "DATE_TRUNC('YEAR', orderdate)")),
                        List.of(Column.column("revenue", WrenTypes.INTEGER, null, true, "sum(totalprice)")),
                        List.of()))
                .build();
        WrenMDL mdl = WrenMDL.fromManifest(copyOf(manifest).setMetrics(metrics).build());

        List<List<Object>> result = query(rewrite("SELECT * FROM testMetricOnMetric ORDER BY orderyear", mdl));
        assertThat(result.get(0).size()).isEqualTo(2);
        assertThat(result.size()).isEqualTo(7);

        assertThatCode(() -> query(rewrite("select 1 from testMetricOnMetric", mdl, true)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testAggregatePrimaryKey()
    {
        List<List<Object>> result = query(rewrite("select * from CountOrderkey limit 10"));
        assertThat(result.get(0).size()).isEqualTo(3);
        assertThat(result.size()).isEqualTo(10);
    }

    @Test
    public void testDynamicMetricOnModel()
    {
        // select all in CountOrders
        assertThat(query(rewrite("SELECT * FROM CountOrders WHERE custkey = 370", true)))
                .isEqualTo(query("SELECT custkey, orderstatus, count(*) FROM orders WHERE custkey = 370 GROUP BY 1, 2"));
        assertThat(query(rewrite("SELECT * FROM CountOrders WHERE custkey = 370", true)))
                .isEqualTo(query(rewrite("SELECT * FROM CountOrders WHERE custkey = 370", false)));

        // select dim in CountOrders
        assertThat(query(rewrite("SELECT custkey FROM CountOrders WHERE custkey = 370", true)))
                .isEqualTo(query("WITH output AS (SELECT custkey, count(*) FROM orders WHERE custkey = 370 GROUP BY 1) SELECT custkey FROM output"));

        // select measure in CountOrders
        assertThat(query(rewrite("SELECT count FROM CountOrders WHERE custkey = 370", true)))
                .isEqualTo(query("WITH output AS (SELECT custkey, count(*) AS count FROM orders WHERE custkey = 370 GROUP BY 1) SELECT count FROM output"));

        // select dim custkey and measure count in CountOrders
        assertThat(query(rewrite("SELECT custkey, count FROM CountOrders WHERE custkey = 370", true)))
                .isEqualTo(query("SELECT custkey, count(*) FROM orders WHERE custkey = 370 GROUP BY 1"));

        // select only measure will use all dimension
        assertThat(query(rewrite("SELECT count FROM CountOrders ORDER BY 1", true)))
                .isEqualTo(query("WITH output AS (SELECT count(*) AS count FROM orders) SELECT count FROM output ORDER BY 1"));

        // apply count(*) on metric
        assertThat(query(rewrite("SELECT count(*) FROM CountOrders ORDER BY 1", true)))
                .isEqualTo(query("WITH output AS (SELECT custkey, orderstatus, count(*) AS count FROM orders GROUP BY 1, 2) SELECT count(*) FROM output"));

        // apply count(custkey) on metric
        assertThat(query(rewrite("SELECT count(custkey) FROM CountOrders ORDER BY 1", true)))
                .isEqualTo(query("WITH output AS (SELECT custkey, count(*) FROM orders GROUP BY 1) SELECT count(custkey) FROM output"));

        assertThatThrownBy(() -> query(rewrite("SELECT count(custkey) FROM notfound ORDER BY 1", true)))
                .rootCause()
                .hasMessageMatching(".*Table with name notfound does not exist(.|\\n)*");
    }

    @Test
    public void testDynamicMetricOnMetric()
    {
        // select all in CountOrders2
        assertThat(query(rewrite("SELECT * FROM CountOrders2 WHERE orderstatus = 'F'", true)))
                .isEqualTo(query("SELECT orderstatus, CAST(count(*) AS HUGEINT) FROM orders WHERE orderstatus = 'F' GROUP BY 1"));
        assertThat(query(rewrite("SELECT * FROM CountOrders2 WHERE orderstatus = 'F'", true)))
                .isEqualTo(query(rewrite("SELECT * FROM CountOrders2 WHERE orderstatus = 'F'", false)));

        // select dim in CountOrders2
        assertThat(query(rewrite("SELECT orderstatus FROM CountOrders2 WHERE orderstatus = 'F'", true)))
                .isEqualTo(query("WITH output AS (SELECT orderstatus, CAST(count(*) AS HUGEINT) FROM orders WHERE orderstatus = 'F' GROUP BY 1)\n" +
                        "SELECT orderstatus FROM output"));

        // select measure in CountOrders2
        assertThat(query(rewrite("SELECT sum_count FROM CountOrders2 WHERE orderstatus = 'F'", true)))
                .isEqualTo(query("WITH output AS (SELECT orderstatus, CAST(count(*) AS HUGEINT) AS sum_count FROM orders WHERE orderstatus = 'F' GROUP BY 1)\n" +
                        "SELECT sum_count FROM output"));
    }

    @Test
    public void testInvokeFunctionInDimension()
    {
        assertThat(query(rewrite("SELECT name, orderdate, totalprice FROM TotalpriceByCustomerYear WHERE name = 'Customer#000001276' ORDER BY 1, 2", true)))
                .isEqualTo(query("""
                        SELECT name, date_trunc('YEAR', orderdate), sum(totalprice)
                        FROM orders JOIN customer ON orders.custkey = customer.custkey
                        WHERE name = 'Customer#000001276'
                        GROUP BY 1, 2
                        ORDER BY 1, 2
                        """));
        assertThat(query(rewrite("SELECT name, orderdate, totalprice FROM TotalpriceByCustomerBaseOrdersYear WHERE name = 'Customer#000001276' ORDER BY 1, 2", true)))
                .isEqualTo(query("""
                        SELECT name, date_trunc('YEAR', orderdate), sum(totalprice)
                        FROM orders JOIN customer ON orders.custkey = customer.custkey
                        WHERE name = 'Customer#000001276'
                        GROUP BY 1, 2
                        ORDER BY 1, 2
                        """));
    }

    private String rewrite(String sql)
    {
        return rewrite(sql, wrenMDL);
    }

    private String rewrite(String sql, boolean enableDynamicField)
    {
        return rewrite(sql, wrenMDL, enableDynamicField);
    }

    private String rewrite(String sql, WrenMDL wrenMDL)
    {
        return WrenPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, new AnalyzedMDL(wrenMDL, null), List.of(WREN_SQL_REWRITE));
    }

    private String rewrite(String sql, WrenMDL wrenMDL, boolean enableDynamicField)
    {
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog("wren")
                .setSchema("test")
                .setEnableDynamic(enableDynamicField)
                .build();
        String result = WrenPlanner.rewrite(sql, sessionContext, new AnalyzedMDL(wrenMDL, null), List.of(WREN_SQL_REWRITE));
        System.out.println(result);
        return result;
    }
}
