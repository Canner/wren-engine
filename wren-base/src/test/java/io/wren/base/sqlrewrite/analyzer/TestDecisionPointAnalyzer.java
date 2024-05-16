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

package io.wren.base.sqlrewrite.analyzer;

import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.Statement;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.Manifest;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionPointAnalyzer;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.FilterAnalysis;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.FilterAnalysis.ExpressionAnalysis;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.FilterAnalysis.LogicalAnalysis;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.QueryAnalysis;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.RelationAnalysis;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.RelationAnalysis.JoinRelation;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.RelationAnalysis.SubqueryRelation;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.RelationAnalysis.TableRelation;
import org.testng.annotations.Test;

import java.util.List;

import static io.wren.base.dto.Model.onTableReference;
import static io.wren.base.dto.TableReference.tableReference;
import static io.wren.base.sqlrewrite.Utils.parseSql;
import static io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionExpressionAnalyzer.INCLUDE_FUNCTION_CALL;
import static io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionExpressionAnalyzer.INCLUDE_MATHEMATICAL_OPERATION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDecisionPointAnalyzer
{
    private static final SessionContext DEFAULT_SESSION_CONTEXT =
            SessionContext.builder().setCatalog("test").setSchema("test").build();
    private WrenMDL mdl;

    public TestDecisionPointAnalyzer()
    {
        List<Column> customerColumns = List.of(
                Column.column("custkey", WrenTypes.INTEGER, null, true),
                Column.column("name", WrenTypes.VARCHAR, null, true),
                Column.column("address", WrenTypes.VARCHAR, null, true),
                Column.column("nationkey", WrenTypes.INTEGER, null, true),
                Column.column("phone", WrenTypes.VARCHAR, null, true),
                Column.column("acctbal", WrenTypes.INTEGER, null, true),
                Column.column("mktsegment", WrenTypes.VARCHAR, null, true),
                Column.column("comment", WrenTypes.VARCHAR, null, true));
        List<Column> ordersColumns = List.of(
                Column.column("orderkey", WrenTypes.INTEGER, null, true),
                Column.column("custkey", WrenTypes.INTEGER, null, true),
                Column.column("orderstatus", WrenTypes.VARCHAR, null, true),
                Column.column("totalprice", WrenTypes.INTEGER, null, true),
                Column.column("orderdate", WrenTypes.DATE, null, true),
                Column.column("orderpriority", WrenTypes.VARCHAR, null, true),
                Column.column("clerk", WrenTypes.VARCHAR, null, true),
                Column.column("shippriority", WrenTypes.INTEGER, null, true),
                Column.column("comment", WrenTypes.VARCHAR, null, true));
        List<Column> lineitemColumns = List.of(
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
                Column.column("comment", WrenTypes.VARCHAR, null, true));

        mdl = WrenMDL.fromManifest(Manifest.builder()
                .setCatalog(DEFAULT_SESSION_CONTEXT.getCatalog().orElseThrow())
                .setSchema(DEFAULT_SESSION_CONTEXT.getSchema().orElseThrow())
                .setModels(List.of(onTableReference("customer", tableReference(null, "main", "customer"), customerColumns, "custkey"),
                        onTableReference("orders", tableReference(null, "main", "orders"), ordersColumns, "orderkey"),
                        onTableReference("lineitem", tableReference(null, "main", "lineitem"), lineitemColumns, null)))
                .build());
    }

    @Test
    public void testSelectItem()
    {
        Statement statement = parseSql("SELECT custkey, name FROM customer");
        List<QueryAnalysis> result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(2);
        assertThat(result.get(0).getSelectItems().get(0).getExpression()).isEqualTo("custkey");
        assertThat(result.get(0).getSelectItems().get(1).getExpression()).isEqualTo("name");

        statement = parseSql("SELECT * FROM customer");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(8);

        statement = parseSql("SELECT * FROM customer, orders");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(17);

        statement = parseSql("SELECT customer.*, orderkey FROM customer JOIN orders ON customer.custkey = orders.custkey");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(9);

        statement = parseSql("SELECT c.*, orderkey FROM customer c JOIN orders o ON c.custkey = o.custkey");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(9);

        statement = parseSql("SELECT custkey, date_trunc('MONTH', orderdate), custkey + 1,  mod(custkey + 1, 10) FROM orders");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(4);
        assertThat(result.get(0).getSelectItems().get(0).getExpression()).isEqualTo("custkey");
        assertThat(result.get(0).getSelectItems().get(0).getProperties().get(INCLUDE_FUNCTION_CALL)).isEqualTo("false");
        assertThat(result.get(0).getSelectItems().get(0).getProperties().get(INCLUDE_MATHEMATICAL_OPERATION)).isEqualTo("false");
        assertThat(result.get(0).getSelectItems().get(1).getExpression()).isEqualTo("date_trunc('MONTH', orderdate)");
        assertThat(result.get(0).getSelectItems().get(1).getProperties().get(INCLUDE_FUNCTION_CALL)).isEqualTo("true");
        assertThat(result.get(0).getSelectItems().get(1).getProperties().get(INCLUDE_MATHEMATICAL_OPERATION)).isEqualTo("false");
        assertThat(result.get(0).getSelectItems().get(2).getExpression()).isEqualTo("(custkey + 1)");
        assertThat(result.get(0).getSelectItems().get(2).getProperties().get(INCLUDE_FUNCTION_CALL)).isEqualTo("false");
        assertThat(result.get(0).getSelectItems().get(2).getProperties().get(INCLUDE_MATHEMATICAL_OPERATION)).isEqualTo("true");
        assertThat(result.get(0).getSelectItems().get(3).getExpression()).isEqualTo("mod((custkey + 1), 10)");
        assertThat(result.get(0).getSelectItems().get(3).getProperties().get(INCLUDE_FUNCTION_CALL)).isEqualTo("true");
        assertThat(result.get(0).getSelectItems().get(3).getProperties().get(INCLUDE_MATHEMATICAL_OPERATION)).isEqualTo("true");

        statement = parseSql("SELECT custkey ckey, orderkey okey FROM customer");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(2);
        assertThat(result.get(0).getSelectItems().get(0).getExpression()).isEqualTo("custkey");
        assertThat(result.get(0).getSelectItems().get(0).getAliasName().get()).isEqualTo("ckey");
        assertThat(result.get(0).getSelectItems().get(1).getExpression()).isEqualTo("orderkey");
        assertThat(result.get(0).getSelectItems().get(1).getAliasName().get()).isEqualTo("okey");

        statement = parseSql("SELECT * FROM remote_customer");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().get(0).getExpression()).isEqualTo("*");

        statement = parseSql("SELECT c.* FROM remote_customer c");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(1);
        assertThat(result.get(0).getSelectItems().get(0).getExpression()).isEqualTo("c.*");
    }

    @Test
    public void testRelation()
    {
        Statement statement = parseSql("SELECT * FROM customer, orders");
        List<QueryAnalysis> result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.IMPLICIT_JOIN);
        if (result.get(0).getRelation() instanceof JoinRelation) {
            JoinRelation joinRelation = (JoinRelation) result.get(0).getRelation();
            assertThat(joinRelation.getLeft().getAlias()).isNull();
            assertThat(joinRelation.getLeft().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getLeft()).getTableName()).isEqualTo("customer");
            assertThat(joinRelation.getRight().getAlias()).isNull();
            assertThat(joinRelation.getRight().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getRight()).getTableName()).isEqualTo("orders");
        }
        else {
            throw new AssertionError("wrong type");
        }

        statement = parseSql("SELECT * FROM customer JOIN orders ON customer.custkey = orders.custkey");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.INNER_JOIN);
        if (result.get(0).getRelation() instanceof JoinRelation) {
            JoinRelation joinRelation = (JoinRelation) result.get(0).getRelation();
            assertThat(joinRelation.getLeft().getAlias()).isNull();
            assertThat(joinRelation.getLeft().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getLeft()).getTableName()).isEqualTo("customer");
            assertThat(joinRelation.getRight().getAlias()).isNull();
            assertThat(joinRelation.getRight().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getRight()).getTableName()).isEqualTo("orders");
            assertThat(joinRelation.getCriteria()).isEqualTo("ON (customer.custkey = orders.custkey)");
        }
        else {
            throw new AssertionError("wrong type");
        }

        statement = parseSql("SELECT * FROM (customer JOIN orders ON customer.custkey = orders.custkey) join_relation");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getAlias()).isEqualTo("join_relation");
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.INNER_JOIN);
        if (result.get(0).getRelation() instanceof JoinRelation) {
            JoinRelation joinRelation = (JoinRelation) result.get(0).getRelation();
            assertThat(joinRelation.getLeft().getAlias()).isNull();
            assertThat(joinRelation.getLeft().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getLeft()).getTableName()).isEqualTo("customer");
            assertThat(joinRelation.getRight().getAlias()).isNull();
            assertThat(joinRelation.getRight().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getRight()).getTableName()).isEqualTo("orders");
            assertThat(joinRelation.getCriteria()).isEqualTo("ON (customer.custkey = orders.custkey)");
        }
        else {
            throw new AssertionError("wrong type");
        }

        statement = parseSql("SELECT * FROM customer JOIN orders ON customer.custkey = orders.custkey LEFT JOIN lineitem ON orders.orderkey = lineitem.orderkey");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.LEFT_JOIN);
        if (result.get(0).getRelation() instanceof JoinRelation) {
            JoinRelation joinRelation = (JoinRelation) result.get(0).getRelation();
            assertThat(joinRelation.getLeft().getAlias()).isNull();
            assertThat(joinRelation.getLeft().getType()).isEqualTo(RelationAnalysis.Type.INNER_JOIN);
            assertThat(((JoinRelation) joinRelation.getLeft()).getLeft().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((JoinRelation) joinRelation.getLeft()).getRight().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(joinRelation.getRight().getAlias()).isNull();
            assertThat(joinRelation.getRight().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getRight()).getTableName()).isEqualTo("lineitem");
            assertThat(joinRelation.getCriteria()).isEqualTo("ON (orders.orderkey = lineitem.orderkey)");
        }
        else {
            throw new AssertionError("wrong type");
        }

        statement = parseSql("SELECT * FROM customer JOIN orders USING (custkey)");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.INNER_JOIN);
        if (result.get(0).getRelation() instanceof JoinRelation) {
            JoinRelation joinRelation = (JoinRelation) result.get(0).getRelation();
            assertThat(joinRelation.getLeft().getAlias()).isNull();
            assertThat(joinRelation.getLeft().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getLeft()).getTableName()).isEqualTo("customer");
            assertThat(joinRelation.getRight().getAlias()).isNull();
            assertThat(joinRelation.getRight().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getRight()).getTableName()).isEqualTo("orders");
            assertThat(joinRelation.getCriteria()).isEqualTo("USING (custkey)");
        }
        else {
            throw new AssertionError("wrong type");
        }

        statement = parseSql("SELECT * FROM customer JOIN customer USING (custkey, name)");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.INNER_JOIN);
        if (result.get(0).getRelation() instanceof JoinRelation) {
            JoinRelation joinRelation = (JoinRelation) result.get(0).getRelation();
            assertThat(joinRelation.getLeft().getAlias()).isNull();
            assertThat(joinRelation.getLeft().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getLeft()).getTableName()).isEqualTo("customer");
            assertThat(joinRelation.getRight().getAlias()).isNull();
            assertThat(joinRelation.getRight().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(((TableRelation) joinRelation.getRight()).getTableName()).isEqualTo("customer");
            assertThat(joinRelation.getCriteria()).isEqualTo("USING (custkey, name)");
        }
        else {
            throw new AssertionError("wrong type");
        }
    }

    @Test
    public void testFilter()
    {
        Statement statement = parseSql("SELECT * FROM customer WHERE custkey = 1");
        List<QueryAnalysis> result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getFilter().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
        if (result.get(0).getFilter() instanceof ExpressionAnalysis) {
            ExpressionAnalysis expressionAnalysis = (ExpressionAnalysis) result.get(0).getFilter();
            assertThat(expressionAnalysis.getNode()).isEqualTo("(custkey = 1)");
        }
        else {
            throw new AssertionError("wrong type");
        }

        statement = parseSql("SELECT * FROM customer WHERE custkey = 1 AND name = 'test'");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getFilter().getType()).isEqualTo(FilterAnalysis.Type.AND);
        if (result.get(0).getFilter() instanceof LogicalAnalysis) {
            LogicalAnalysis logicalAnalysis = (LogicalAnalysis) result.get(0).getFilter();
            assertThat(logicalAnalysis.getLeft().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
            assertThat(((ExpressionAnalysis) logicalAnalysis.getLeft()).getNode()).isEqualTo("(custkey = 1)");
            assertThat(logicalAnalysis.getRight().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
            assertThat(((ExpressionAnalysis) logicalAnalysis.getRight()).getNode()).isEqualTo("(name = 'test')");
        }

        statement = parseSql("SELECT * FROM customer WHERE custkey = 1 OR name = 'test'");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getFilter().getType()).isEqualTo(FilterAnalysis.Type.OR);
        if (result.get(0).getFilter() instanceof LogicalAnalysis) {
            LogicalAnalysis logicalAnalysis = (LogicalAnalysis) result.get(0).getFilter();
            assertThat(logicalAnalysis.getLeft().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
            assertThat(((ExpressionAnalysis) logicalAnalysis.getLeft()).getNode()).isEqualTo("(custkey = 1)");
            assertThat(logicalAnalysis.getRight().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
            assertThat(((ExpressionAnalysis) logicalAnalysis.getRight()).getNode()).isEqualTo("(name = 'test')");
        }

        statement = parseSql("SELECT * FROM customer WHERE custkey = 1 OR (name = 'test' AND address = 'test')");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getFilter().getType()).isEqualTo(FilterAnalysis.Type.OR);
        if (result.get(0).getFilter() instanceof LogicalAnalysis) {
            LogicalAnalysis logicalAnalysis = (LogicalAnalysis) result.get(0).getFilter();
            assertThat(logicalAnalysis.getLeft().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
            assertThat(((ExpressionAnalysis) logicalAnalysis.getLeft()).getNode()).isEqualTo("(custkey = 1)");
            assertThat(logicalAnalysis.getRight().getType()).isEqualTo(FilterAnalysis.Type.AND);
            if (logicalAnalysis.getRight() instanceof LogicalAnalysis) {
                LogicalAnalysis andLogicalAnalysis = (LogicalAnalysis) logicalAnalysis.getRight();
                assertThat(andLogicalAnalysis.getLeft().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
                assertThat(((ExpressionAnalysis) andLogicalAnalysis.getLeft()).getNode()).isEqualTo("(name = 'test')");
                assertThat(andLogicalAnalysis.getRight().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
                assertThat(((ExpressionAnalysis) andLogicalAnalysis.getRight()).getNode()).isEqualTo("(address = 'test')");
            }
        }

        statement = parseSql("SELECT * FROM customer WHERE custkey = 1 OR name = 'test' AND address = 'test'");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getFilter().getType()).isEqualTo(FilterAnalysis.Type.OR);
        if (result.get(0).getFilter() instanceof LogicalAnalysis) {
            LogicalAnalysis logicalAnalysis = (LogicalAnalysis) result.get(0).getFilter();
            assertThat(logicalAnalysis.getLeft().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
            assertThat(((ExpressionAnalysis) logicalAnalysis.getLeft()).getNode()).isEqualTo("(custkey = 1)");
            assertThat(logicalAnalysis.getRight().getType()).isEqualTo(FilterAnalysis.Type.AND);
            if (logicalAnalysis.getRight() instanceof LogicalAnalysis) {
                LogicalAnalysis andLogicalAnalysis = (LogicalAnalysis) logicalAnalysis.getRight();
                assertThat(andLogicalAnalysis.getLeft().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
                assertThat(((ExpressionAnalysis) andLogicalAnalysis.getLeft()).getNode()).isEqualTo("(name = 'test')");
                assertThat(andLogicalAnalysis.getRight().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
                assertThat(((ExpressionAnalysis) andLogicalAnalysis.getRight()).getNode()).isEqualTo("(address = 'test')");
            }
        }

        statement = parseSql("SELECT * FROM customer WHERE custkey = 1 OR name = 'test' AND if(nationkey = 1 OR nationkey = 2, true, false)");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getFilter().getType()).isEqualTo(FilterAnalysis.Type.OR);
        if (result.get(0).getFilter() instanceof LogicalAnalysis) {
            LogicalAnalysis logicalAnalysis = (LogicalAnalysis) result.get(0).getFilter();
            assertThat(logicalAnalysis.getLeft().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
            assertThat(((ExpressionAnalysis) logicalAnalysis.getLeft()).getNode()).isEqualTo("(custkey = 1)");
            assertThat(logicalAnalysis.getRight().getType()).isEqualTo(FilterAnalysis.Type.AND);
            if (logicalAnalysis.getRight() instanceof LogicalAnalysis) {
                LogicalAnalysis andLogicalAnalysis = (LogicalAnalysis) logicalAnalysis.getRight();
                assertThat(andLogicalAnalysis.getLeft().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
                assertThat(((ExpressionAnalysis) andLogicalAnalysis.getLeft()).getNode()).isEqualTo("(name = 'test')");
                assertThat(andLogicalAnalysis.getRight().getType()).isEqualTo(FilterAnalysis.Type.EXPR);
                assertThat(((ExpressionAnalysis) andLogicalAnalysis.getRight()).getNode()).isEqualTo("IF(((nationkey = 1) OR (nationkey = 2)), true, false)");
            }
        }
    }

    @Test
    public void testGroupBy()
    {
        Statement statement = parseSql("SELECT custkey, count(*) FROM customer GROUP BY custkey");
        List<QueryAnalysis> result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().get(0).get(0)).isEqualTo("custkey");

        statement = parseSql("SELECT c.custkey, count(*) FROM customer c GROUP BY c.custkey");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().get(0).get(0)).isEqualTo("c.custkey");

        statement = parseSql("SELECT custkey, count(*) FROM customer GROUP BY custkey, name");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().size()).isEqualTo(2);
        assertThat(result.get(0).getGroupByKeys().get(0).get(0)).isEqualTo("custkey");
        assertThat(result.get(0).getGroupByKeys().get(1).get(0)).isEqualTo("name");

        statement = parseSql("SELECT custkey, count(*) FROM customer GROUP BY (custkey, name), nationkey");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().size()).isEqualTo(2);
        assertThat(result.get(0).getGroupByKeys().get(0).get(0)).isEqualTo("custkey");
        assertThat(result.get(0).getGroupByKeys().get(0).get(1)).isEqualTo("name");
        assertThat(result.get(0).getGroupByKeys().get(1).get(0)).isEqualTo("nationkey");

        statement = parseSql("SELECT custkey, count(*) FROM customer GROUP BY 1");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().get(0).get(0)).isEqualTo("custkey");

        statement = parseSql("SELECT c.custkey, count(*) FROM customer c GROUP BY 1");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().get(0).get(0)).isEqualTo("c.custkey");

        statement = parseSql("SELECT custkey, count(*), name FROM customer GROUP BY 1, 3, nationkey");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getGroupByKeys().size()).isEqualTo(3);
        assertThat(result.get(0).getGroupByKeys().get(0).get(0)).isEqualTo("custkey");
        assertThat(result.get(0).getGroupByKeys().get(1).get(0)).isEqualTo("name");
        assertThat(result.get(0).getGroupByKeys().get(2).get(0)).isEqualTo("nationkey");
    }

    @Test
    public void testSorting()
    {
        Statement statement = parseSql("SELECT custkey, name FROM customer ORDER BY custkey");
        List<QueryAnalysis> result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSortings().size()).isEqualTo(1);
        assertThat(result.get(0).getSortings().get(0).getExpression()).isEqualTo("custkey");
        assertThat(result.get(0).getSortings().get(0).getOrdering()).isEqualTo(SortItem.Ordering.ASCENDING);

        statement = parseSql("SELECT custkey, name FROM customer ORDER BY custkey ASC, name DESC");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSortings().size()).isEqualTo(2);
        assertThat(result.get(0).getSortings().get(0).getExpression()).isEqualTo("custkey");
        assertThat(result.get(0).getSortings().get(0).getOrdering()).isEqualTo(SortItem.Ordering.ASCENDING);
        assertThat(result.get(0).getSortings().get(1).getExpression()).isEqualTo("name");
        assertThat(result.get(0).getSortings().get(1).getOrdering()).isEqualTo(SortItem.Ordering.DESCENDING);

        statement = parseSql("SELECT custkey, name FROM customer ORDER BY 1 ASC, 2 DESC");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getSortings().size()).isEqualTo(2);
        assertThat(result.get(0).getSortings().get(0).getExpression()).isEqualTo("custkey");
        assertThat(result.get(0).getSortings().get(0).getOrdering()).isEqualTo(SortItem.Ordering.ASCENDING);
        assertThat(result.get(0).getSortings().get(1).getExpression()).isEqualTo("name");
        assertThat(result.get(0).getSortings().get(1).getOrdering()).isEqualTo(SortItem.Ordering.DESCENDING);
    }

    @Test
    public void testMultipleQuery()
    {
        Statement statement = parseSql("WITH t1 as (SELECT * FROM customer) SELECT * FROM t1");
        List<QueryAnalysis> result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
        assertThat(result.get(0).getRelation().getAlias()).isNull();
        assertThat(result.get(0).getSelectItems().size()).isEqualTo(8);
        if (result.get(0).getRelation() instanceof TableRelation tableRelation) {
            assertThat(tableRelation.getTableName()).isEqualTo("customer");
        }

        assertThat(result.get(1).getRelation().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
        assertThat(result.get(1).getRelation().getAlias()).isNull();
        assertThat(result.get(1).getSelectItems().size()).isEqualTo(8);
        if (result.get(1).getRelation() instanceof TableRelation tableRelation) {
            assertThat(tableRelation.getTableName()).isEqualTo("t1");
        }
    }

    @Test
    public void testSubQuery()
    {
        Statement statement = parseSql("SELECT * FROM (SELECT * FROM customer) t1");
        List<QueryAnalysis> result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.SUBQUERY);
        assertThat(result.get(0).getRelation().getAlias()).isEqualTo("t1");

        statement = parseSql("SELECT * FROM (WITH t1 AS (SELECT * FROM customer) SELECT * FROM t1) t2");
        result = DecisionPointAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, mdl);
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.SUBQUERY);
        assertThat(result.get(0).getRelation().getAlias()).isEqualTo("t2");
        if (result.get(0).getRelation() instanceof SubqueryRelation subQueryRelation) {
            assertThat(subQueryRelation.getBody().size()).isEqualTo(2);
            assertThat(subQueryRelation.getBody().get(0).getRelation().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
            assertThat(subQueryRelation.getBody().get(1).getRelation().getType()).isEqualTo(RelationAnalysis.Type.TABLE);
        }
    }
}
