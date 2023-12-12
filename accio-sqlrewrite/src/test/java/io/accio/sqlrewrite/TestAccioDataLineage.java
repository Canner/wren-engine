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
import io.accio.base.dto.Relationship;
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
import static io.accio.base.dto.JoinType.MANY_TO_ONE;
import static io.accio.base.dto.JoinType.ONE_TO_MANY;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.testing.AbstractTestFramework.addColumnsToModel;
import static io.accio.testing.AbstractTestFramework.withDefaultCatalogSchema;
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
                caluclatedColumn("extended_price", BIGINT, "sum(lineitem.extendedprice)"));
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(newCustomer, newOrders, lineitem))
                .setRelationships(List.of(ordersCustomer, ordersLineitem))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(manifest);

        AccioDataLineage dataLineage = AccioDataLineage.analyze(mdl);
        LinkedHashMap<String, Set<String>> actual = dataLineage.getRequiredFields(QualifiedName.of("Customer", "total_price"));
        LinkedHashMap<String, Set<String>> expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("totalprice", "custkey"));
        expected.put("Customer", Set.of("total_price"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Orders", "customer_name"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("customer_name"));
        expected.put("Customer", Set.of("name", "custkey"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Customer", "discount_extended_price"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("extended_price", "custkey"));
        expected.put("Lineitem", Set.of("discount", "extendedprice", "orderkey"));
        expected.put("Customer", Set.of("discount_extended_price"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(
                ImmutableList.of(
                        QualifiedName.of("Customer", "total_price"),
                        QualifiedName.of("Customer", "discount_extended_price")));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("extended_price", "totalprice", "custkey"));
        expected.put("Lineitem", Set.of("discount", "extendedprice", "orderkey"));
        expected.put("Customer", Set.of("discount_extended_price", "total_price"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(
                ImmutableList.of(
                        QualifiedName.of("Customer", "total_price"),
                        QualifiedName.of("Orders", "extended_price")));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("extended_price", "totalprice", "custkey"));
        expected.put("Lineitem", Set.of("extendedprice", "orderkey"));
        expected.put("Customer", Set.of("total_price"));
        assertThat(actual).isEqualTo(expected);

        actual = dataLineage.getRequiredFields(QualifiedName.of("Customer", "lineitem_price"));
        expected = new LinkedHashMap<>();
        expected.put("Orders", Set.of("custkey"));
        expected.put("Lineitem", Set.of("extendedprice", "discount", "orderkey"));
        expected.put("Customer", Set.of("lineitem_price"));
        assertThat(actual).isEqualTo(expected);

        assertThatThrownBy(
                () -> dataLineage.getRequiredFields(
                        ImmutableList.of(QualifiedName.of("Customer", "total_price"), QualifiedName.of("Orders", "customer_name"))))
                .hasMessage("found cycle in Customer.total_price");
    }
}
