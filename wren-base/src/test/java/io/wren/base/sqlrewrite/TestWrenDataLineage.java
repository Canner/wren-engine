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
import io.trino.sql.tree.QualifiedName;
import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.CumulativeMetric;
import io.wren.base.dto.JoinType;
import io.wren.base.dto.Manifest;
import io.wren.base.dto.Measure;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationship;
import io.wren.base.dto.TimeUnit;
import io.wren.base.dto.Window;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.wren.base.sqlrewrite.AbstractTestFramework.addColumnsToModel;
import static io.wren.base.sqlrewrite.AbstractTestFramework.withDefaultCatalogSchema;
import static io.wren.base.sqlrewrite.ModelInfo.ORIGINAL_SUFFIX;
import static io.wren.base.sqlrewrite.WrenDataLineage.RelationableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestWrenDataLineage
{
    private final Model customer;
    private final Model orders;
    private final Model lineitem;
    private final Relationship ordersCustomer;
    private final Relationship ordersLineitem;

    private final RelationableReference customerRef;
    private final RelationableReference ordersRef;
    private final RelationableReference lineitemRef;
    private final RelationableReference customerOriginalRef;
    private final RelationableReference ordersOriginalRef;
    private final RelationableReference lineitemOriginalRef;

    public TestWrenDataLineage()
    {
        customer = Model.model("Customer",
                "select * from main.customer",
                List.of(
                        Column.column("custkey", WrenTypes.INTEGER, null, true),
                        Column.column("name", WrenTypes.VARCHAR, null, true),
                        Column.column("address", WrenTypes.VARCHAR, null, true),
                        Column.column("nationkey", WrenTypes.INTEGER, null, true),
                        Column.column("phone", WrenTypes.VARCHAR, null, true),
                        Column.column("acctbal", WrenTypes.INTEGER, null, true),
                        Column.column("mktsegment", WrenTypes.VARCHAR, null, true),
                        Column.column("comment", WrenTypes.VARCHAR, null, true)),
                "custkey");
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
        lineitem = Model.model("Lineitem",
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
                        Column.column("orderkey_linenumber", WrenTypes.VARCHAR, null, true, "concat(orderkey, '-', linenumber)")),
                "orderkey_linenumber");
        ordersCustomer = Relationship.relationship("OrdersCustomer", List.of("Orders", "Customer"), JoinType.MANY_TO_ONE, "Orders.custkey = Customer.custkey");
        ordersLineitem = Relationship.relationship("OrdersLineitem", List.of("Orders", "Lineitem"), JoinType.ONE_TO_MANY, "Orders.orderkey = Lineitem.orderkey");

        customerRef = new RelationableReference(Optional.of(customer), customer.getName(), false);
        ordersRef = new RelationableReference(Optional.of(orders), orders.getName(), false);
        lineitemRef = new RelationableReference(Optional.of(lineitem), lineitem.getName(), false);
        customerOriginalRef = new RelationableReference(Optional.of(customer), customer.getName() + ORIGINAL_SUFFIX, true);
        ordersOriginalRef = new RelationableReference(Optional.of(orders), orders.getName() + ORIGINAL_SUFFIX, true);
        lineitemOriginalRef = new RelationableReference(Optional.of(lineitem), lineitem.getName() + ORIGINAL_SUFFIX, true);
    }

    @Test
    public void testAnalyze()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                Column.column("orders", "Orders", "OrdersCustomer", true),
                Column.caluclatedColumn("total_price", WrenTypes.BIGINT, "sum(orders.totalprice)"),
                Column.caluclatedColumn("discount_extended_price", WrenTypes.BIGINT, "sum(orders.lineitem.discount + orders.extended_price)"),
                Column.caluclatedColumn("lineitem_price", WrenTypes.BIGINT, "sum(orders.lineitem.discount * orders.lineitem.extendedprice)"));
        Model newOrders = addColumnsToModel(
                orders,
                Column.column("customer", "Customer", "OrdersCustomer", true),
                Column.column("lineitem", "Lineitem", "OrdersLineitem", true),
                Column.caluclatedColumn("customer_name", WrenTypes.BIGINT, "customer.name"),
                Column.caluclatedColumn("extended_price", WrenTypes.BIGINT, "sum(lineitem.extendedprice)"),
                Column.caluclatedColumn("extended_price_2", WrenTypes.BIGINT, "sum(lineitem.extendedprice + totalprice)"));
        Model newLineitem = addColumnsToModel(
                lineitem,
                Column.column("orders", "Orders", "OrdersLineitem", true),
                Column.caluclatedColumn("test_column", WrenTypes.BIGINT, "orders.customer.total_price + extendedprice"));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, newOrders, newLineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        WrenMDL mdl = WrenMDL.fromManifest(manifest);

        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);
        LinkedHashMap<RelationableReference, Set<String>> actual;
        LinkedHashMap<RelationableReference, Set<String>> expected;
        actual = dataLineage.getRequiredFields(QualifiedName.of("Customer", "total_price"));
        expected = new LinkedHashMap<>();
        expected.put(ordersOriginalRef, Set.of("totalprice"));
        expected.put(customerOriginalRef, Set.of("orders"));
        expected.put(customerRef, Set.of("total_price"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Orders", "customer_name"));
        expected = new LinkedHashMap<>();
        expected.put(ordersRef, Set.of("customer_name"));
        expected.put(ordersOriginalRef, Set.of("customer"));
        expected.put(customerOriginalRef, Set.of("name"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Customer", "discount_extended_price"));
        expected = new LinkedHashMap<>();
        expected.put(ordersRef, Set.of("extended_price"));
        expected.put(ordersOriginalRef, Set.of("lineitem"));
        expected.put(lineitemOriginalRef, Set.of("discount", "extendedprice"));
        expected.put(customerRef, Set.of("discount_extended_price"));
        expected.put(customerOriginalRef, Set.of("orders"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(
                ImmutableList.of(
                        QualifiedName.of("Customer", "total_price"),
                        QualifiedName.of("Customer", "discount_extended_price")));
        expected = new LinkedHashMap<>();
        expected.put(ordersRef, Set.of("extended_price"));
        expected.put(ordersOriginalRef, Set.of("lineitem", "totalprice"));
        expected.put(lineitemOriginalRef, Set.of("discount", "extendedprice"));
        expected.put(customerRef, Set.of("total_price", "discount_extended_price"));
        expected.put(customerOriginalRef, Set.of("orders"));

        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(
                ImmutableList.of(
                        QualifiedName.of("Customer", "total_price"),
                        QualifiedName.of("Orders", "extended_price")));
        expected = new LinkedHashMap<>();
        expected.put(ordersRef, Set.of("extended_price"));
        expected.put(ordersOriginalRef, Set.of("totalprice", "lineitem"));
        expected.put(lineitemOriginalRef, Set.of("extendedprice"));
        expected.put(customerRef, Set.of("total_price"));
        expected.put(customerOriginalRef, Set.of("orders"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Customer", "lineitem_price"));
        expected = new LinkedHashMap<>();
        expected.put(ordersOriginalRef, Set.of("lineitem"));
        expected.put(lineitemOriginalRef, Set.of("extendedprice", "discount"));
        expected.put(customerRef, Set.of("lineitem_price"));
        expected.put(customerOriginalRef, Set.of("orders"));
        assertThat(actual).isEqualTo(expected);

        // assert cycle
        assertThatNoException().isThrownBy(
                () -> dataLineage.getRequiredFields(
                        ImmutableList.of(QualifiedName.of("Customer", "total_price"), QualifiedName.of("Orders", "customer_name"))));

        actual = dataLineage.getRequiredFields(QualifiedName.of("Orders", "extended_price_2"));
        expected = new LinkedHashMap<>();
        expected.put(ordersRef, Set.of("extended_price_2"));
        expected.put(ordersOriginalRef, Set.of("totalprice", "lineitem"));
        expected.put(lineitemOriginalRef, Set.of("extendedprice"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Lineitem", "test_column"));
        expected = new LinkedHashMap<>();
        expected.put(customerRef, Set.of("total_price"));
        expected.put(customerOriginalRef, Set.of("orders"));
        expected.put(ordersOriginalRef, Set.of("customer", "totalprice"));
        expected.put(lineitemRef, Set.of("test_column"));
        expected.put(lineitemOriginalRef, Set.of("orders", "extendedprice"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeModelOnModel()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                Column.column("orders", "Orders", "OrdersCustomer", true),
                Column.caluclatedColumn("total_price", WrenTypes.BIGINT, "sum(orders.totalprice)"));
        Model onCustomer = Model.onBaseObject(
                "OnCustomer",
                "Customer",
                ImmutableList.of(
                        Column.column("mom_name", "VARCHAR", null, true, "name"),
                        Column.column("mom_custkey", "VARCHAR", null, true, "custkey"),
                        Column.column("mom_totalprice", "VARCHAR", null, true, "total_price")),
                "mom_custkey");
        Model newOrders = addColumnsToModel(
                orders,
                Column.column("on_customer", "OnCustomer", "OrdersOnCustomer", true),
                Column.caluclatedColumn("customer_name", WrenTypes.BIGINT, "on_customer.mom_name"));
        Relationship ordersOnCustomer = Relationship.relationship("OrdersOnCustomer", List.of("Orders", "OnCustomer"), JoinType.MANY_TO_ONE, "Orders.custkey = OnCustomer.mom_custkey");
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newOrders, newCustomer, onCustomer))
                .setRelationships(List.of(ordersOnCustomer, ordersCustomer))
                .build();
        RelationableReference onCustomerRef = new RelationableReference(Optional.of(onCustomer), onCustomer.getName(), false);
        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);

        LinkedHashMap<RelationableReference, Set<String>> actual;
        LinkedHashMap<RelationableReference, Set<String>> expected;

        actual = dataLineage.getRequiredFields(QualifiedName.of("OnCustomer", "mom_totalprice"));
        expected = new LinkedHashMap<>();
        expected.put(ordersOriginalRef, Set.of("totalprice"));
        expected.put(customerOriginalRef, Set.of("orders"));
        expected.put(customerRef, Set.of("total_price"));
        expected.put(onCustomerRef, Set.of("mom_totalprice"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Orders", "customer_name"));
        expected = new LinkedHashMap<>();
        expected.put(ordersOriginalRef, Set.of("on_customer"));
        expected.put(ordersRef, Set.of("customer_name"));
        expected.put(customerOriginalRef, Set.of("name"));
        expected.put(onCustomerRef, Set.of("mom_name"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeMetricOnModel()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                Column.column("orders", "Orders", "OrdersCustomer", true));
        Metric customerSpending = Metric.metric("CustomerSpending", "Customer",
                List.of(Column.column("name", WrenTypes.VARCHAR, null, true)),
                List.of(Column.column("spending", WrenTypes.BIGINT, null, true, "sum(orders.totalprice)"),
                        Column.column("count", WrenTypes.BIGINT, null, true, "count(*)")));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders, newCustomer))
                .setMetrics(List.of(customerSpending))
                .setRelationships(List.of(ordersCustomer))
                .build();
        RelationableReference customerSpendingRef = new RelationableReference(Optional.of(customerSpending), customerSpending.getName(), false);

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);
        LinkedHashMap<RelationableReference, Set<String>> actual;
        LinkedHashMap<RelationableReference, Set<String>> expected;
        actual = dataLineage.getRequiredFields(QualifiedName.of("CustomerSpending", "name"));
        expected = new LinkedHashMap<>();
        expected.put(customerOriginalRef, Set.of("name"));
        expected.put(customerSpendingRef, Set.of("name"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("CustomerSpending", "count"));
        expected = new LinkedHashMap<>();
        expected.put(customerRef, Set.of());
        expected.put(customerSpendingRef, Set.of("count"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("CustomerSpending", "spending"));
        expected = new LinkedHashMap<>();
        expected.put(customerOriginalRef, Set.of("orders"));
        expected.put(ordersOriginalRef, Set.of("totalprice"));
        expected.put(customerSpendingRef, Set.of("spending"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("CustomerSpending", "name"), QualifiedName.of("CustomerSpending", "spending")));
        expected = new LinkedHashMap<>();
        expected.put(customerOriginalRef, Set.of("orders", "name"));
        expected.put(ordersOriginalRef, Set.of("totalprice"));
        expected.put(customerSpendingRef, Set.of("name", "spending"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAnalyzeMetricOnMetric()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                Column.column("orders", "Orders", "OrdersCustomer", true));
        Metric customerSpending = Metric.metric("CustomerSpending", "Customer",
                List.of(Column.column("name", WrenTypes.VARCHAR, null, true),
                        Column.column("address", WrenTypes.VARCHAR, null, true)),
                List.of(Column.column("spending", WrenTypes.BIGINT, null, true, "sum(orders.totalprice)")));
        Metric derived = Metric.metric("Derived", "CustomerSpending",
                List.of(Column.column("address", WrenTypes.VARCHAR, null, true)),
                List.of(Column.column("spending", WrenTypes.BIGINT, null, true, "sum(spending)")));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders, newCustomer))
                .setMetrics(List.of(customerSpending, derived))
                .setRelationships(List.of(ordersCustomer))
                .build();
        RelationableReference customerSpendingRef = new RelationableReference(Optional.of(customerSpending), customerSpending.getName(), false);
        RelationableReference derivedRef = new RelationableReference(Optional.of(derived), derived.getName(), false);

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);
        LinkedHashMap<RelationableReference, Set<String>> actual;
        LinkedHashMap<RelationableReference, Set<String>> expected;
        actual = dataLineage.getRequiredFields(QualifiedName.of("Derived", "address"));
        expected = new LinkedHashMap<>();
        expected.put(customerOriginalRef, Set.of("address"));
        expected.put(customerSpendingRef, Set.of("address"));
        expected.put(derivedRef, Set.of("address"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Derived", "spending"));
        expected = new LinkedHashMap<>();
        expected.put(customerOriginalRef, Set.of("orders"));
        expected.put(ordersOriginalRef, Set.of("totalprice"));
        expected.put(customerSpendingRef, Set.of("spending"));
        expected.put(derivedRef, Set.of("spending"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("Derived", "address"), QualifiedName.of("Derived", "spending")));
        expected = new LinkedHashMap<>();
        expected.put(customerOriginalRef, Set.of("orders", "address"));
        expected.put(ordersOriginalRef, Set.of("totalprice"));
        expected.put(customerSpendingRef, Set.of("address", "spending"));
        expected.put(derivedRef, Set.of("address", "spending"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test(enabled = false)
    public void testAnalyzeCumulativeMetricOnModel()
    {
        CumulativeMetric dailyRevenue = CumulativeMetric.cumulativeMetric("DailyRevenue", "Orders",
                Measure.measure("c_totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                Window.window("c_orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));
        RelationableReference dailyRevenueRef = new RelationableReference(Optional.empty(), dailyRevenue.getName(), false);

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setCumulativeMetrics(List.of(dailyRevenue))
                .build();

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);
        LinkedHashMap<RelationableReference, Set<String>> actual;
        LinkedHashMap<RelationableReference, Set<String>> expected;

        actual = dataLineage.getRequiredFields(QualifiedName.of("DailyRevenue", "c_totalprice"));
        expected = new LinkedHashMap<>();
        expected.put(ordersOriginalRef, Set.of("totalprice"));
        expected.put(dailyRevenueRef, Set.of("c_totalprice"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(List.of(QualifiedName.of("DailyRevenue", "c_totalprice"), QualifiedName.of("DailyRevenue", "c_orderdate")));
        expected = new LinkedHashMap<>();
        expected.put(ordersOriginalRef, Set.of("totalprice", "orderdate"));
        expected.put(dailyRevenueRef, Set.of("c_totalprice", "c_orderdate"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test(enabled = false)
    public void testAnalyzeCumulativeMetricOnMetric()
    {
        Metric totalpriceByDate = Metric.metric("TotalpriceByDate", "Orders",
                List.of(Column.column("orderdate", WrenTypes.DATE, null, true)),
                List.of(Column.column("totalprice", WrenTypes.INTEGER, null, true, "sum(totalprice)")));
        CumulativeMetric dailyRevenue = CumulativeMetric.cumulativeMetric("DailyRevenue", "TotalpriceByDate",
                Measure.measure("c_totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                Window.window("c_orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setMetrics(List.of(totalpriceByDate))
                .setCumulativeMetrics(List.of(dailyRevenue))
                .build();

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);
        LinkedHashMap<RelationableReference, Set<String>> actual;
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

    @Test(enabled = false)
    public void testAnalyzeCumulativeMetricOnCumulativeMetric()
    {
        CumulativeMetric dailyRevenue = CumulativeMetric.cumulativeMetric("DailyRevenue", "Orders",
                Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                Window.window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));
        CumulativeMetric dailyRevenue2 = CumulativeMetric.cumulativeMetric("DailyRevenue2", "DailyRevenue",
                Measure.measure("c_totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                Window.window("c_orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setCumulativeMetrics(List.of(dailyRevenue, dailyRevenue2))
                .build();

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);
        LinkedHashMap<RelationableReference, Set<String>> actual;
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

    @Test(enabled = false)
    public void testAnalyzeModelOnCumulativeMetric()
    {
        CumulativeMetric dailyRevenue = CumulativeMetric.cumulativeMetric("DailyRevenue", "Orders",
                Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                Window.window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));
        Model onDailyRevenue = Model.onBaseObject("OnDailyRevenue", "DailyRevenue",
                ImmutableList.of(
                        Column.column("c_totalprice", WrenTypes.INTEGER, null, true, "totalprice"),
                        Column.column("c_orderdate", WrenTypes.DATE, null, true, "orderdate")),
                "orderdate");

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(onDailyRevenue, orders))
                .setCumulativeMetrics(List.of(dailyRevenue))
                .build();

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);
        LinkedHashMap<RelationableReference, Set<String>> actual;
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

    @Test(enabled = false)
    public void testAnalyzeMetricOnCumulativeMetric()
    {
        CumulativeMetric dailyRevenue = CumulativeMetric.cumulativeMetric("DailyRevenue", "Orders",
                Measure.measure("totalprice", WrenTypes.INTEGER, "sum", "totalprice"),
                Window.window("orderdate", "orderdate", TimeUnit.DAY, "1994-01-01", "1994-12-31"));
        Metric onDailyRevenue = Metric.metric("OnDailyRevenue", "DailyRevenue",
                ImmutableList.of(Column.column("c_orderdate", WrenTypes.DATE, null, true, "orderdate")),
                ImmutableList.of(Column.column("c_totalprice", WrenTypes.INTEGER, null, true, "sum(totalprice)")));

        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(orders))
                .setMetrics(List.of(onDailyRevenue))
                .setCumulativeMetrics(List.of(dailyRevenue))
                .build();

        WrenMDL mdl = WrenMDL.fromManifest(manifest);
        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);
        LinkedHashMap<RelationableReference, Set<String>> actual;
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
    public void testGetSourceColumns()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                Column.column("orders", "Orders", "OrdersCustomer", true),
                Column.caluclatedColumn("discount_extended_price", WrenTypes.BIGINT, "sum(orders.lineitem.discount + orders.lineitem.extendedprice)"));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, orders, lineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        WrenMDL mdl = WrenMDL.fromManifest(manifest);

        WrenDataLineage dataLineage = WrenDataLineage.analyze(mdl);
        Map<String, Set<String>> actual;
        Map<String, Set<String>> expected;
        actual = dataLineage.getSourceColumns(QualifiedName.of("Customer", "discount_extended_price"));
        expected = new HashMap<>();
        expected.put("Customer", Set.of("orders"));
        expected.put("Orders", Set.of("lineitem"));
        expected.put("Lineitem", Set.of("extendedprice", "discount"));
        assertThat(actual).isEqualTo(expected);

        // assert not exist
        assertThat(dataLineage.getSourceColumns(QualifiedName.of("foo", "bar")).size()).isEqualTo(0);
    }
}
