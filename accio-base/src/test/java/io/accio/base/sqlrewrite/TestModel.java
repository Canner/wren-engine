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
import io.accio.base.AnalyzedMDL;
import io.accio.base.SessionContext;
import io.accio.base.dto.Manifest;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioTypes.BIGINT;
import static io.accio.base.AccioTypes.DATE;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.caluclatedColumn;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.JoinType.MANY_TO_ONE;
import static io.accio.base.dto.JoinType.ONE_TO_MANY;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Model.onBaseObject;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.base.sqlrewrite.AccioSqlRewrite.ACCIO_SQL_REWRITE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestModel
        extends AbstractTestFramework
{
    private final Model customer;
    private final Model orders;
    private final Model lineitem;
    private final Relationship ordersCustomer;
    private final Relationship ordersLineitem;

    public TestModel()
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

    @Override
    protected void prepareData()
    {
        String orders = requireNonNull(getClass().getClassLoader().getResource("tiny-orders.parquet")).getPath();
        exec("create table orders as select * from '" + orders + "'");
        String customer = requireNonNull(getClass().getClassLoader().getResource("tiny-customer.parquet")).getPath();
        exec("create table customer as select * from '" + customer + "'");
        String lineitem = requireNonNull(getClass().getClassLoader().getResource("tiny-lineitem.parquet")).getPath();
        exec("create table lineitem as select * from '" + lineitem + "'");
    }

    @Test
    public void testToManyCalculated()
    {
        // TODO: add this to test case, currently this won't work
        // caluclatedColumn("col_3", BIGINT, "concat(address, sum(orders.lineitem.discount * orders.lineitem.extendedprice))");

        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true),
                caluclatedColumn("totalprice", BIGINT, "sum(orders.totalprice)"),
                caluclatedColumn("buy_item_count", BIGINT, "count(distinct orders.lineitem.orderkey_linenumber)"),
                caluclatedColumn("lineitem_totalprice", BIGINT, "sum(orders.lineitem.discount * orders.lineitem.extendedprice)"),
                caluclatedColumn("test_col", BIGINT, "sum(orders.lineitem.discount * nationkey)"));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, orders, lineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);

        assertQuery(mdl,
                "SELECT totalprice FROM Customer WHERE custkey = 370",
                "SELECT sum(totalprice) FROM customer c LEFT JOIN orders o ON c.custkey = o.custkey WHERE c.custkey = 370");
        assertQuery(mdl,
                "SELECT custkey, buy_item_count FROM Customer WHERE custkey = 370",
                "SELECT c.custkey, count(*) FROM customer c " +
                        "LEFT JOIN orders o ON c.custkey = o.custkey " +
                        "LEFT JOIN lineitem l ON o.orderkey = l.orderkey " +
                        "WHERE c.custkey = 370 " +
                        "GROUP BY 1");
        assertQuery(mdl,
                "SELECT custkey, lineitem_totalprice FROM Customer WHERE custkey = 370",
                "SELECT c.custkey, sum(l.extendedprice * l.discount) FROM customer c " +
                        "LEFT JOIN orders o ON c.custkey = o.custkey " +
                        "LEFT JOIN lineitem l ON o.orderkey = l.orderkey " +
                        "WHERE c.custkey = 370 " +
                        "GROUP BY 1");

        assertQuery(mdl,
                "SELECT custkey, test_col FROM Customer WHERE custkey = 370",
                "SELECT c.custkey, sum(l.discount * c.nationkey) FROM customer c " +
                        "LEFT JOIN orders o ON c.custkey = o.custkey " +
                        "LEFT JOIN lineitem l ON o.orderkey = l.orderkey " +
                        "WHERE c.custkey = 370 " +
                        "GROUP BY 1");
    }

    @Test
    public void testToOneCalculated()
    {
        Model newLineitem = addColumnsToModel(
                lineitem,
                column("orders", "Orders", "OrdersLineitem", true),
                caluclatedColumn("col_1", BIGINT, "orders.totalprice + orders.totalprice"),
                caluclatedColumn("col_2", BIGINT, "concat(orders.orderkey, '#', orders.customer.custkey)"));
        Model newOrders = addColumnsToModel(
                orders,
                column("customer", "Customer", "OrdersCustomer", true));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(customer, newOrders, newLineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);

        assertQuery(mdl,
                "SELECT col_1 FROM Lineitem WHERE orderkey = 44995",
                "SELECT (totalprice + totalprice) AS col_1\n" +
                        "FROM lineitem l\n" +
                        "LEFT JOIN orders o ON l.orderkey = o.orderkey\n" +
                        "WHERE l.orderkey = 44995");
        assertQuery(mdl,
                "SELECT col_1 FROM Lineitem WHERE orderkey = 44995",
                "SELECT (totalprice + totalprice) AS col_1\n" +
                        "FROM lineitem l\n" +
                        "LEFT JOIN orders o ON l.orderkey = o.orderkey\n" +
                        "WHERE l.orderkey = 44995");
        assertQuery(mdl,
                "SELECT col_2 FROM Lineitem WHERE orderkey = 44995",
                "SELECT concat(l.orderkey, '#', c.custkey) AS col_2\n" +
                        "FROM lineitem l\n" +
                        "LEFT JOIN orders o ON l.orderkey = o.orderkey\n" +
                        "LEFT JOIN customer c ON o.custkey = c.custkey\n" +
                        "WHERE l.orderkey = 44995");
    }

    @Test
    public void testModelWithCycle()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true),
                caluclatedColumn("total_price", BIGINT, "sum(orders.totalprice)"));
        Model newOrders = addColumnsToModel(
                orders,
                column("customer", "Customer", "OrdersCustomer", true),
                caluclatedColumn("customer_name", BIGINT, "customer.name"));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, newOrders))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);

        assertThatCode(() -> query(rewrite("SELECT * FROM Orders", mdl, true)))
                .doesNotThrowAnyException();
        assertThatCode(() -> query(rewrite("SELECT customer_name FROM Orders WHERE orderkey = 44995", mdl, true)))
                .doesNotThrowAnyException();
        assertThatCode(() -> query(rewrite("SELECT total_price FROM Customer", mdl, true)))
                .doesNotThrowAnyException();
        assertThatCode(() -> query(rewrite("SELECT total_price FROM Customer c LEFT JOIN Orders o ON c.custkey = o.custkey", mdl, true)))
                .doesNotThrowAnyException();
        assertThatCode(() -> query(rewrite("SELECT o.custkey, total_price FROM Customer c LEFT JOIN Orders o ON c.custkey = o.custkey", mdl, true)))
                .doesNotThrowAnyException();
        assertThatCode(() -> query(rewrite("SELECT customer_name, total_price FROM Customer c LEFT JOIN Orders o ON c.custkey = o.custkey", mdl, true)))
                .hasMessageMatching("found cycle in .*");
    }

    @Test
    public void testModelOnModel()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true),
                caluclatedColumn("totalprice", BIGINT, "sum(orders.totalprice)"));
        Model onCustomer = onBaseObject(
                "OnCustomer",
                "Customer",
                ImmutableList.of(
                        column("mom_custkey", "VARCHAR", null, true, "custkey"),
                        column("mom_totalprice", "VARCHAR", null, true, "totalprice")),
                "mom_custkey");
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, onCustomer, orders))
                .setRelationships(List.of(ordersCustomer))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);

        assertQuery(mdl, "SELECT mom_custkey, mom_totalprice FROM OnCustomer WHERE mom_custkey = 370",
                "SELECT c.custkey, sum(o.totalprice) FROM customer c\n" +
                        "LEFT JOIN orders o ON c.custkey = o.custkey\n" +
                        "WHERE c.custkey = 370\n" +
                        "GROUP BY 1");

        assertThatCode(() -> query(rewrite("SELECT 1 FROM OnCustomer", mdl, true)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testCalculatedUseAnotherCalculated()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true),
                caluclatedColumn("total_price", BIGINT, "sum(orders.totalprice)"));
        Model newOrders = addColumnsToModel(
                orders,
                column("customer", "Customer", "OrdersCustomer", true),
                column("lineitem", "Lineitem", "OrdersLineitem", true),
                caluclatedColumn("customer_name", BIGINT, "customer.name"),
                caluclatedColumn("extended_price", BIGINT, "sum(lineitem.extendedprice)"));
        Model newLineitem = addColumnsToModel(
                lineitem,
                column("orders", "Orders", "OrdersLineitem", true),
                caluclatedColumn("test_column", BIGINT, "orders.customer.total_price + extendedprice"));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, newOrders, newLineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);

        assertThatCode(() -> query(rewrite("SELECT test_column FROM Lineitem", mdl, true)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testSelectEmpty()
    {
        Model newCustomer = addColumnsToModel(
                customer,
                column("orders", "Orders", "OrdersCustomer", true),
                caluclatedColumn("total_price", BIGINT, "sum(orders.totalprice)"));
        Model newOrders = addColumnsToModel(
                orders,
                column("customer", "Customer", "OrdersCustomer", true),
                column("lineitem", "Lineitem", "OrdersLineitem", true),
                caluclatedColumn("customer_name", BIGINT, "customer.name"),
                caluclatedColumn("extended_price", BIGINT, "sum(lineitem.extendedprice)"));
        Model newLineitem = addColumnsToModel(
                lineitem,
                column("orders", "Orders", "OrdersLineitem", true),
                caluclatedColumn("test_column", BIGINT, "orders.customer.total_price + extendedprice"));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, newOrders, newLineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);

        assertThatCode(() -> query(rewrite("SELECT true \"_\" FROM Lineitem", mdl, true)))
                .doesNotThrowAnyException();

        assertThatCode(() -> query(rewrite("SELECT true \"_\" FROM Lineitem, Orders", mdl, true)))
                .doesNotThrowAnyException();

        assertThatCode(() -> query(rewrite("SELECT orderkey FROM Lineitem, Orders", mdl, true)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testSelectNotFound()
    {
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(customer))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);
        assertThatThrownBy(() -> query(rewrite("SELECT * FROM notfound", mdl, true)))
                .hasMessageFindingMatch(".*notfound.*");
    }

    private void assertQuery(AccioMDL mdl, @Language("SQL") String accioSql, @Language("SQL") String duckDBSql)
    {
        assertThat(query(rewrite(accioSql, mdl, true))).isEqualTo(query(duckDBSql));
        assertThat(query(rewrite(accioSql, mdl, false))).isEqualTo(query(duckDBSql));
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
