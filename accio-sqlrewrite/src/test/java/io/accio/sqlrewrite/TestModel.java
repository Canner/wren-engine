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
import io.accio.base.dto.Manifest;
import io.accio.testing.AbstractTestFramework;
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
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.sqlrewrite.AccioSqlRewrite.ACCIO_SQL_REWRITE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestModel
        extends AbstractTestFramework
{
    private final AccioMDL accioMDL;

    public TestModel()
    {
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(
                        model("Customer",
                                "select * from main.customer",
                                List.of(
                                        column("pk", INTEGER, null, true, "concat(custkey, name)"),
                                        column("custkey", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        column("address", VARCHAR, null, true),
                                        column("nationkey", INTEGER, null, true),
                                        column("phone", VARCHAR, null, true),
                                        column("acctbal", INTEGER, null, true),
                                        column("mktsegment", VARCHAR, null, true),
                                        column("comment", VARCHAR, null, true),
                                        column("orders", "Orders", "OrdersCustomer", true),
                                        caluclatedColumn("totalprice", BIGINT, "sum(orders.totalprice)"),
                                        caluclatedColumn("buy_item_count", BIGINT, "count(distinct orders.lineitem.orderkey_linenumber)"),
                                        caluclatedColumn("lineitem_totalprice", BIGINT, "sum(orders.lineitem.discount * orders.lineitem.extendedprice)")),
                                "pk"),
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
                                        column("lineitem", "Lineitem", "OrdersLineitem", true)),
                                "orderkey"),
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
                                        column("orderkey_linenumber", VARCHAR, null, true, "concat(orderkey, '-', linenumber)")),
                                "orderkey_linenumber")))
                .setRelationships(List.of(
                        relationship("OrdersCustomer", List.of("Orders", "Customer"), MANY_TO_ONE, "Orders.custkey = Customer.custkey"),
                        relationship("OrdersLineitem", List.of("Orders", "Lineitem"), ONE_TO_MANY, "Orders.orderkey = Lineitem.orderkey")))
                .build();
        accioMDL = AccioMDL.fromManifest(manifest);
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
    public void testCalculated()
    {
        assertThat(query(rewrite("SELECT totalprice FROM Customer WHERE custkey = 370")))
                .isEqualTo(query("SELECT sum(totalprice) FROM customer c LEFT JOIN orders o ON c.custkey = o.custkey WHERE c.custkey = 370"));
        assertThat(query(rewrite("SELECT custkey, buy_item_count FROM Customer WHERE custkey = 370")))
                .isEqualTo(query(
                        "SELECT c.custkey, count(*) FROM customer c " +
                                "LEFT JOIN orders o ON c.custkey = o.custkey " +
                                "LEFT JOIN lineitem l ON o.orderkey = l.orderkey " +
                                "WHERE c.custkey = 370 " +
                                "GROUP BY 1"));
        assertThat(query(rewrite("SELECT custkey, lineitem_totalprice FROM Customer WHERE custkey = 370")))
                .isEqualTo(query(
                        "SELECT c.custkey, sum(l.extendedprice * l.discount) FROM customer c " +
                                "LEFT JOIN orders o ON c.custkey = o.custkey " +
                                "LEFT JOIN lineitem l ON o.orderkey = l.orderkey " +
                                "WHERE c.custkey = 370 " +
                                "GROUP BY 1"));
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
