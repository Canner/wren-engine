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

import com.google.common.collect.ImmutableList;
import io.accio.base.AccioMDL;
import io.accio.base.dto.CumulativeMetric;
import io.accio.base.dto.Manifest;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import io.accio.base.dto.TimeUnit;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static io.accio.base.AccioTypes.BIGINT;
import static io.accio.base.AccioTypes.DATE;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.caluclatedColumn;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.CumulativeMetric.cumulativeMetric;
import static io.accio.base.dto.JoinType.MANY_TO_ONE;
import static io.accio.base.dto.JoinType.ONE_TO_MANY;
import static io.accio.base.dto.Measure.measure;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Model.onBaseObject;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.base.dto.Window.window;
import static io.accio.base.sqlrewrite.AbstractTestFramework.addColumnsToModel;
import static io.accio.base.sqlrewrite.AbstractTestFramework.withDefaultCatalogSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAccioDataLineage
{
    private final Model customer;
    private final Model orders;
    private final Model lineitem;
    private final Relationship ordersCustomer;
    private final Relationship ordersLineitem;

    public TestAccioDataLineage()
    {
        customer = model("Customer",
                "select * from main.customer",
                List.of(
                        column("custkey", INTEGER, null, true),
                        column("name", VARCHAR, null, true),
                        column("address", VARCHAR, null, true),
                        column("nationkey", INTEGER, null, true),
                        column("phone", VARCHAR, null, true),
                        column("acctbal", INTEGER, null, true),
                        column("mktsegment", VARCHAR, null, true),
                        column("comment", VARCHAR, null, true)),
                "custkey");
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
        lineitem = model("Lineitem",
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
                        column("orderkey_linenumber", VARCHAR, null, true, "concat(orderkey, '-', linenumber)")),
                "orderkey_linenumber");
        ordersCustomer = relationship("OrdersCustomer", List.of("Orders", "Customer"), MANY_TO_ONE, "Orders.custkey = Customer.custkey");
        ordersLineitem = relationship("OrdersLineitem", List.of("Orders", "Lineitem"), ONE_TO_MANY, "Orders.orderkey = Lineitem.orderkey");
    }

    @Test
    public void testAnalyze()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true),
                caluclatedColumn("total_price", BIGINT, "sum(orders.totalprice)"),
                caluclatedColumn("discount_extended_price", BIGINT, "sum(orders.lineitem.discount + orders.extended_price)"),
                caluclatedColumn("lineitem_price", BIGINT, "sum(orders.lineitem.discount * orders.lineitem.extendedprice)"));
        Model newOrders = addColumnsToModel(
                orders,
                column("customer", "Customer", "OrdersCustomer", true),
                column("lineitem", "Lineitem", "OrdersLineitem", true),
                caluclatedColumn("customer_name", BIGINT, "customer.name"),
                caluclatedColumn("extended_price", BIGINT, "sum(lineitem.extendedprice)"),
                caluclatedColumn("extended_price_2", BIGINT, "sum(lineitem.extendedprice + totalprice)"));
        Model newLineitem = addColumnsToModel(
                lineitem,
                column("orders", "Orders", "OrdersLineitem", true),
                caluclatedColumn("test_column", BIGINT, "orders.customer.total_price + extendedprice"));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, newOrders, newLineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);

        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);
        LinkedHashMap<String, Set<String>> actual;
        LinkedHashMap<String, Set<String>> expected;
        actual = dataLineage.getRequiredFields(QualifiedName.of("Customer", "total_price"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice", "custkey"));
        expected.put("Customer", Set.of("custkey", "total_price"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Orders", "customer_name"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("custkey", "customer_name"));
        expected.put("Customer", Set.of("name", "custkey"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Customer", "discount_extended_price"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("extended_price", "custkey", "orderkey"));
        expected.put("Lineitem", Set.of("discount", "extendedprice", "orderkey"));
        expected.put("Customer", Set.of("custkey", "discount_extended_price"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(
                ImmutableList.of(
                        QualifiedName.of("Customer", "total_price"),
                        QualifiedName.of("Customer", "discount_extended_price")));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("extended_price", "orderkey", "custkey", "totalprice"));
        expected.put("Lineitem", Set.of("discount", "extendedprice", "orderkey"));
        expected.put("Customer", Set.of("custkey", "total_price", "discount_extended_price"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(
                ImmutableList.of(
                        QualifiedName.of("Customer", "total_price"),
                        QualifiedName.of("Orders", "extended_price")));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("custkey", "orderkey", "totalprice", "extended_price"));
        expected.put("Lineitem", Set.of("extendedprice", "orderkey"));
        expected.put("Customer", Set.of("custkey", "total_price"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Customer", "lineitem_price"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("custkey", "orderkey"));
        expected.put("Lineitem", Set.of("extendedprice", "discount", "orderkey"));
        expected.put("Customer", Set.of("custkey", "lineitem_price"));
        assertThat(actual).isEqualTo(expected);

        // assert cycle
        assertThatThrownBy(
                () -> dataLineage.getRequiredFields(
                        ImmutableList.of(QualifiedName.of("Customer", "total_price"), QualifiedName.of("Orders", "customer_name"))))
                .hasMessage("found cycle in Customer.total_price");

        actual = dataLineage.getRequiredFields(QualifiedName.of("Orders", "extended_price_2"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("orderkey", "totalprice", "extended_price_2"));
        expected.put("Lineitem", Set.of("extendedprice", "orderkey"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Lineitem", "test_column"));
        expected = new LinkedHashMap<>();
        expected.put("Customer", Set.of("custkey", "total_price"));
        expected.put("Orders", Set.of("custkey", "orderkey", "totalprice"));
        expected.put("Lineitem", Set.of("extendedprice", "orderkey", "test_column"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeModelOnModel()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true),
                caluclatedColumn("total_price", BIGINT, "sum(orders.totalprice)"));
        Model onCustomer = onBaseObject(
                "OnCustomer",
                "Customer",
                ImmutableList.of(
                        column("mom_name", "VARCHAR", null, true, "name"),
                        column("mom_custkey", "VARCHAR", null, true, "custkey"),
                        column("mom_totalprice", "VARCHAR", null, true, "total_price")),
                "mom_custkey");
        Model newOrders = addColumnsToModel(
                orders,
                column("on_customer", "OnCustomer", "OrdersOnCustomer", true),
                caluclatedColumn("customer_name", BIGINT, "on_customer.mom_name"));
        Relationship ordersOnCustomer = relationship("OrdersOnCustomer", List.of("Orders", "OnCustomer"), MANY_TO_ONE, "Orders.custkey = OnCustomer.mom_custkey");
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newOrders, newCustomer, onCustomer))
                .setRelationships(List.of(ordersOnCustomer, ordersCustomer))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);

        LinkedHashMap<String, Set<String>> actual;
        LinkedHashMap<String, Set<String>> expected;

        actual = dataLineage.getRequiredFields(QualifiedName.of("OnCustomer", "mom_totalprice"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("custkey", "totalprice"));
        expected.put("Customer", Set.of("custkey", "total_price"));
        expected.put("OnCustomer", Set.of("mom_totalprice"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Orders", "customer_name"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("custkey", "customer_name"));
        expected.put("Customer", Set.of("custkey", "name"));
        expected.put("OnCustomer", Set.of("mom_custkey", "mom_name"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeMetricOnModel()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true));
        Metric customerSpending = metric("CustomerSpending", "Customer",
                List.of(column("name", VARCHAR, null, true)),
                List.of(column("spending", BIGINT, null, true, "sum(orders.totalprice)"),
                        column("count", BIGINT, null, true, "count(*)")));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders, newCustomer))
                .setMetrics(List.of(customerSpending))
                .setRelationships(List.of(ordersCustomer))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);
        LinkedHashMap<String, Set<String>> actual;
        LinkedHashMap<String, Set<String>> expected;
        actual = dataLineage.getRequiredFields(QualifiedName.of("CustomerSpending", "name"));
        expected = new LinkedHashMap<>();
        expected.put("Customer", Set.of("name"));
        expected.put("CustomerSpending", Set.of("name"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("CustomerSpending", "count"));
        expected = new LinkedHashMap<>();
        expected.put("Customer", Set.of());
        expected.put("CustomerSpending", Set.of("count"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("CustomerSpending", "spending"));
        expected = new LinkedHashMap<>();
        expected.put("Customer", Set.of("custkey"));
        expected.put("Orders", Set.of("custkey", "totalprice"));
        expected.put("CustomerSpending", Set.of("spending"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("CustomerSpending", "name"), QualifiedName.of("CustomerSpending", "spending")));
        expected = new LinkedHashMap<>();
        expected.put("Customer", Set.of("custkey", "name"));
        expected.put("Orders", Set.of("custkey", "totalprice"));
        expected.put("CustomerSpending", Set.of("name", "spending"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeMetricOnMetric()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true));
        Metric customerSpending = metric("CustomerSpending", "Customer",
                List.of(column("name", VARCHAR, null, true),
                        column("address", VARCHAR, null, true)),
                List.of(column("spending", BIGINT, null, true, "sum(orders.totalprice)")));
        Metric derived = metric("Derived", "CustomerSpending",
                List.of(column("address", VARCHAR, null, true)),
                List.of(column("spending", BIGINT, null, true, "sum(spending)")));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders, newCustomer))
                .setMetrics(List.of(customerSpending, derived))
                .setRelationships(List.of(ordersCustomer))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);
        LinkedHashMap<String, Set<String>> actual;
        LinkedHashMap<String, Set<String>> expected;
        actual = dataLineage.getRequiredFields(QualifiedName.of("Derived", "address"));
        expected = new LinkedHashMap<>();
        expected.put("Customer", Set.of("address"));
        expected.put("CustomerSpending", Set.of("address"));
        expected.put("Derived", Set.of("address"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Derived", "spending"));
        expected = new LinkedHashMap<>();
        expected.put("Customer", Set.of("custkey"));
        expected.put("Orders", Set.of("custkey", "totalprice"));
        expected.put("CustomerSpending", Set.of("spending"));
        expected.put("Derived", Set.of("spending"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("Derived", "address"), QualifiedName.of("Derived", "spending")));
        expected = new LinkedHashMap<>();
        expected.put("Customer", Set.of("custkey", "address"));
        expected.put("Orders", Set.of("custkey", "totalprice"));
        expected.put("CustomerSpending", Set.of("address", "spending"));
        expected.put("Derived", Set.of("address", "spending"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeCumulativeMetricOnModel()
    {
        CumulativeMetric dailyRevenue = cumulativeMetric("DailyRevenue", "Orders",
                measure("c_totalprice", INTEGER, "sum", "totalprice"),
                window("c_orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setCumulativeMetrics(List.of(dailyRevenue))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);
        LinkedHashMap<String, Set<String>> actual;
        LinkedHashMap<String, Set<String>> expected;

        actual = dataLineage.getRequiredFields(QualifiedName.of("DailyRevenue", "c_totalprice"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice"));
        expected.put("DailyRevenue", Set.of("c_totalprice"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("DailyRevenue", "c_totalprice"), QualifiedName.of("DailyRevenue", "c_orderdate")));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice", "orderdate"));
        expected.put("DailyRevenue", Set.of("c_totalprice", "c_orderdate"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeCumulativeMetricOnMetric()
    {
        Metric totalpriceByDate = metric("TotalpriceByDate", "Orders",
                List.of(column("orderdate", DATE, null, true)),
                List.of(column("totalprice", INTEGER, null, true, "sum(totalprice)")));
        CumulativeMetric dailyRevenue = cumulativeMetric("DailyRevenue", "TotalpriceByDate",
                measure("c_totalprice", INTEGER, "sum", "totalprice"),
                window("c_orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setMetrics(List.of(totalpriceByDate))
                .setCumulativeMetrics(List.of(dailyRevenue))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);
        LinkedHashMap<String, Set<String>> actual;
        LinkedHashMap<String, Set<String>> expected;

        actual = dataLineage.getRequiredFields(QualifiedName.of("DailyRevenue", "c_totalprice"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice"));
        expected.put("TotalpriceByDate", Set.of("totalprice"));
        expected.put("DailyRevenue", Set.of("c_totalprice"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("DailyRevenue", "c_totalprice"), QualifiedName.of("DailyRevenue", "c_orderdate")));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice", "orderdate"));
        expected.put("TotalpriceByDate", Set.of("totalprice", "orderdate"));
        expected.put("DailyRevenue", Set.of("c_totalprice", "c_orderdate"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeCumulativeMetricOnCumulativeMetric()
    {
        CumulativeMetric dailyRevenue = cumulativeMetric("DailyRevenue", "Orders",
                measure("totalprice", INTEGER, "sum", "totalprice"),
                window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));
        CumulativeMetric dailyRevenue2 = cumulativeMetric("DailyRevenue2", "DailyRevenue",
                measure("c_totalprice", INTEGER, "sum", "totalprice"),
                window("c_orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setCumulativeMetrics(List.of(dailyRevenue, dailyRevenue2))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);
        LinkedHashMap<String, Set<String>> actual;
        LinkedHashMap<String, Set<String>> expected;

        actual = dataLineage.getRequiredFields(QualifiedName.of("DailyRevenue2", "c_totalprice"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice"));
        expected.put("DailyRevenue", Set.of("totalprice"));
        expected.put("DailyRevenue2", Set.of("c_totalprice"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("DailyRevenue2", "c_totalprice"), QualifiedName.of("DailyRevenue2", "c_orderdate")));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice", "orderdate"));
        expected.put("DailyRevenue", Set.of("totalprice", "orderdate"));
        expected.put("DailyRevenue2", Set.of("c_totalprice", "c_orderdate"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeModelOnCumulativeMetric()
    {
        CumulativeMetric dailyRevenue = cumulativeMetric("DailyRevenue", "Orders",
                measure("totalprice", INTEGER, "sum", "totalprice"),
                window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));
        Model onDailyRevenue = onBaseObject("OnDailyRevenue", "DailyRevenue",
                ImmutableList.of(
                        column("c_totalprice", INTEGER, null, true, "totalprice"),
                        column("c_orderdate", DATE, null, true, "orderdate")),
                "orderdate");

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(onDailyRevenue, orders))
                .setCumulativeMetrics(List.of(dailyRevenue))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);
        LinkedHashMap<String, Set<String>> actual;
        LinkedHashMap<String, Set<String>> expected;

        actual = dataLineage.getRequiredFields(QualifiedName.of("OnDailyRevenue", "c_totalprice"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice"));
        expected.put("DailyRevenue", Set.of("totalprice"));
        expected.put("OnDailyRevenue", Set.of("c_totalprice"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("OnDailyRevenue", "c_totalprice"), QualifiedName.of("OnDailyRevenue", "c_orderdate")));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice", "orderdate"));
        expected.put("DailyRevenue", Set.of("totalprice", "orderdate"));
        expected.put("OnDailyRevenue", Set.of("c_totalprice", "c_orderdate"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeMetricOnCumulativeMetric()
    {
        CumulativeMetric dailyRevenue = cumulativeMetric("DailyRevenue", "Orders",
                measure("totalprice", INTEGER, "sum", "totalprice"),
                window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));
        Metric onDailyRevenue = metric("OnDailyRevenue", "DailyRevenue",
                ImmutableList.of(column("c_orderdate", DATE, null, true, "orderdate")),
                ImmutableList.of(column("c_totalprice", INTEGER, null, true, "sum(totalprice)")));

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setMetrics(List.of(onDailyRevenue))
                .setCumulativeMetrics(List.of(dailyRevenue))
                .build();

        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);
        LinkedHashMap<String, Set<String>> actual;
        LinkedHashMap<String, Set<String>> expected;

        actual = dataLineage.getRequiredFields(QualifiedName.of("OnDailyRevenue", "c_totalprice"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice"));
        expected.put("DailyRevenue", Set.of("totalprice"));
        expected.put("OnDailyRevenue", Set.of("c_totalprice"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("OnDailyRevenue", "c_totalprice"), QualifiedName.of("OnDailyRevenue", "c_orderdate")));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice", "orderdate"));
        expected.put("DailyRevenue", Set.of("totalprice", "orderdate"));
        expected.put("OnDailyRevenue", Set.of("c_totalprice", "c_orderdate"));
        assertThat(actual).isEqualTo(expected);
    }
}
