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

import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.wren.base.WrenMDL;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationship;
import io.wren.base.sqlrewrite.analyzer.ExpressionRelationshipAnalyzer;
import io.wren.base.sqlrewrite.analyzer.ExpressionRelationshipInfo;
import io.wren.base.sqlrewrite.analyzer.RelationshipColumnInfo;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.wren.base.sqlrewrite.Utils.parseExpression;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExpressionRelationshipRewriter
{
    private WrenMDL mdl;
    private Model orders;
    private Model nation;
    private Relationship ordersCustomer;
    private Relationship customerNation;

    @BeforeClass
    public void init()
            throws IOException
    {
        mdl = WrenMDL.fromJson(Files.readString(Path.of(getClass().getClassLoader().getResource("tpch_mdl.json").getPath())));
        orders = mdl.getModel("Orders").orElseThrow();
        nation = mdl.getModel("Nation").orElseThrow();
        ordersCustomer = mdl.getRelationship("OrdersCustomer").orElseThrow();
        customerNation = mdl.getRelationship("CustomerNation").orElseThrow();
    }

    @DataProvider
    public Object[][] rewriteTests()
    {
        return new Object[][] {
                {"customer.custkey", "\"Customer\".\"custkey\"", List.of(ordersCustomer)},
                {"customer.custkey + customer.custkey", "(\"Customer\".\"custkey\" + \"Customer\".\"custkey\")", List.of(ordersCustomer)},
                {"customer.nation.name", "\"Nation\".\"name\"", List.of(ordersCustomer, customerNation)},
                {"customer.nation.nationkey + 1", "(\"Nation\".\"nationkey\" + 1)", List.of(ordersCustomer, customerNation)},
                {"concat('#', customer.nation.name)", "concat('#', \"Nation\".\"name\")", List.of(ordersCustomer, customerNation)},
                {"concat(customer.name, '#', customer.nation.name)", "concat(\"Customer\".\"name\", '#', \"Nation\".\"name\")",
                        List.of(ordersCustomer, customerNation, ordersCustomer)},
        };
    }

    @Test(dataProvider = "rewriteTests")
    public void testGetToOneRelationshipsRewrite(String actual, String expected, List<Relationship> relationships)
    {
        Expression expression = parseExpression(actual);
        Set<ExpressionRelationshipInfo> expressionRelationshipInfos = ExpressionRelationshipAnalyzer.getToOneRelationships(expression, mdl, orders);
        assertThat(expressionRelationshipInfos.stream().map(ExpressionRelationshipInfo::getRelationships).flatMap(List::stream).collect(toImmutableList()))
                .containsExactlyInAnyOrderElementsOf(relationships);
        assertThat(RelationshipRewriter.rewrite(expressionRelationshipInfos, expression).toString()).isEqualTo(expected);
    }

    @DataProvider
    public Object[][] testGetRelationshipsRewrite()
    {
        return new Object[][] {
                {"customer.custkey", "\"Customer\".\"custkey\"", List.of(Relationship.reverse(customerNation))},
                {"customer.custkey + customer.custkey", "(\"Customer\".\"custkey\" + \"Customer\".\"custkey\")", List.of(Relationship.reverse(customerNation))},
                {"customer.orders.totalprice", "\"Orders\".\"totalprice\"", List.of(Relationship.reverse(customerNation), Relationship.reverse(ordersCustomer))},
                {"customer.orders.totalprice + 1", "(\"Orders\".\"totalprice\" + 1)", List.of(Relationship.reverse(customerNation), Relationship.reverse(ordersCustomer))},
                {"sum(customer.custkey, customer.orders.orderkey)", "sum(\"Customer\".\"custkey\", \"Orders\".\"orderkey\")",
                        List.of(Relationship.reverse(customerNation), Relationship.reverse(customerNation), Relationship.reverse(ordersCustomer))},
        };
    }

    @Test(dataProvider = "testGetRelationshipsRewrite")
    public void testGetRelationshipsRewrite(String actual, String expected, List<Relationship> relationships)
    {
        Expression expression = parseExpression(actual);
        Set<ExpressionRelationshipInfo> expressionRelationshipInfos = ExpressionRelationshipAnalyzer.getRelationships(expression, mdl, nation);
        assertThat(expressionRelationshipInfos.stream().map(ExpressionRelationshipInfo::getRelationships).flatMap(List::stream).collect(toImmutableList()))
                .containsExactlyInAnyOrderElementsOf(relationships);
        assertThat(RelationshipRewriter.rewrite(expressionRelationshipInfos, expression).toString()).isEqualTo(expected);
    }

    @Test
    public void testInvalidGetToOneRelationships()
    {
        assertThatThrownBy(() -> ExpressionRelationshipAnalyzer.getToOneRelationships(parseExpression("customer.custkey"), mdl, nation))
                .hasMessage("expr in model only accept to-one relation");
        assertThatThrownBy(() -> ExpressionRelationshipAnalyzer.getToOneRelationships(parseExpression("customer.nation.customer.custkey"), mdl, orders))
                .hasMessage("expr in model only accept to-one relation");
    }

    @Test
    public void testNoRelationshipFound()
    {
        // won't collect relationship if direct access relationship column
        assertThat(ExpressionRelationshipAnalyzer.getToOneRelationships(parseExpression("customer"), mdl, nation)).isEmpty();
        assertThat(ExpressionRelationshipAnalyzer.getToOneRelationships(parseExpression("customer.nation"), mdl, orders)).isEmpty();
        // won't collect relationship if column not found in model
        assertThat(ExpressionRelationshipAnalyzer.getToOneRelationships(parseExpression("foo"), mdl, orders)).isEmpty();
        // won't collect relationship since "Orders" is not a column in orders model
        assertThat(ExpressionRelationshipAnalyzer.getToOneRelationships(parseExpression("Orders.customer.custkey"), mdl, orders)).isEmpty();
    }

    @Test
    public void testCycle()
    {
        assertThatThrownBy(() -> ExpressionRelationshipAnalyzer.getToOneRelationships(parseExpression("region.nation"), mdl, nation))
                .hasMessage("found cycle in expression");
    }

    @Test
    public void testMetricMeasureRelationship()
    {
        Set<ExpressionRelationshipInfo> infos = ExpressionRelationshipAnalyzer.getRelationships(parseExpression("sum(customer.name)"), mdl, orders).stream().collect(toImmutableSet());

        ExpressionRelationshipInfo expected = new ExpressionRelationshipInfo(
                QualifiedName.of("customer", "name"),
                List.of("customer"),
                List.of("name"),
                List.of(new RelationshipColumnInfo(
                        orders,
                        orders.getColumns().stream().filter(c -> c.getName().equals("customer")).findAny().orElseThrow(),
                        ordersCustomer)),
                ordersCustomer);
        assertThat(infos).containsExactlyInAnyOrder(expected);

        assertThat(RelationshipRewriter.relationshipAware(infos, "count_of_customer", parseExpression("sum(customer.name)")).toString())
                .isEqualTo("sum(\"count_of_customer\".\"name\")");

        infos = ExpressionRelationshipAnalyzer.getRelationships(parseExpression("sum(customer.name) + sum(customer.name)"), mdl, orders).stream().collect(toImmutableSet());
        assertThat(infos).containsExactlyInAnyOrder(expected);
        assertThat(RelationshipRewriter.relationshipAware(infos, "count_of_customer", parseExpression("sum(customer.name)")).toString())
                .isEqualTo("sum(\"count_of_customer\".\"name\")");
    }
}
