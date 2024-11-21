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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.trino.sql.tree.Statement;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.SessionContext;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.JoinType;
import io.wren.base.dto.Manifest;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.sqlrewrite.AbstractTestFramework;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.wren.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.wren.base.WrenMDL.EMPTY;
import static io.wren.base.WrenMDL.fromManifest;
import static io.wren.base.dto.Column.calculatedColumn;
import static io.wren.base.dto.Column.column;
import static io.wren.base.dto.Column.relationshipColumn;
import static io.wren.base.dto.Model.model;
import static io.wren.base.dto.Relationship.relationship;
import static io.wren.base.sqlrewrite.Utils.parseSql;
import static io.wren.base.sqlrewrite.analyzer.StatementAnalyzer.analyze;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatementAnalyzer
        extends AbstractTestFramework
{
    private static final SessionContext DEFAULT_SESSION_CONTEXT =
            SessionContext.builder().setCatalog("test").setSchema("test").build();

    @Test
    public void testValues()
    {
        SessionContext sessionContext = SessionContext.builder().build();
        Statement statement = parseSql("VALUES(1, 'a')");
        Analysis analysis = new Analysis(statement);
        analyze(analysis, statement, sessionContext, EMPTY);

        statement = parseSql("SELECT * FROM (VALUES(1, 'a'))");
        analysis = new Analysis(statement);
        analyze(analysis, statement, sessionContext, EMPTY);
    }

    @Test
    public void testGetTableWithoutWithTable()
    {
        SessionContext sessionContext = SessionContext.builder().setCatalog("test").setSchema("test").build();
        Statement statement = parseSql("WITH a AS (SELECT * FROM People) SELECT * FROM a");
        Analysis analysis = new Analysis(statement);
        analyze(analysis,
                statement,
                sessionContext,
                EMPTY);

        assertThat(analysis.getTables()).containsExactly(new CatalogSchemaTableName("test", "test", "People"));
    }

    @Test
    public void testCollectedColumns()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(ImmutableList.of(
                        Model.model("table_1", "SELECT * FROM foo", ImmutableList.of(varcharColumn("c1"), varcharColumn("c2"))),
                        Model.model("table_2", "SELECT * FROM bar", ImmutableList.of(varcharColumn("c1"), varcharColumn("c2")))))
                .build();

        Multimap<CatalogSchemaTableName, String> expected;
        expected = HashMultimap.create();
        expected.putAll(catalogSchemaTableName("test", "test", "table_1"), ImmutableList.of("c1", "c2"));
        assertThat(analyzeSql("SELECT * FROM table_1", manifest).getCollectedColumns()).isEqualTo(expected);

        expected = HashMultimap.create();
        expected.putAll(catalogSchemaTableName("test", "test", "table_1"), ImmutableList.of("c1"));
        assertThat(analyzeSql("SELECT c1 FROM table_1", manifest).getCollectedColumns()).isEqualTo(expected);

        expected = HashMultimap.create();
        expected.putAll(catalogSchemaTableName("test", "test", "table_1"), ImmutableList.of("c1"));
        assertThat(analyzeSql("SELECT c1, c1 FROM table_1", manifest).getCollectedColumns()).isEqualTo(expected);

        expected = HashMultimap.create();
        expected.putAll(catalogSchemaTableName("test", "test", "table_1"), ImmutableList.of("c1"));
        assertThat(analyzeSql("SELECT t1.c1 FROM table_1 t1", manifest).getCollectedColumns()).isEqualTo(expected);

        expected = HashMultimap.create();
        expected.putAll(catalogSchemaTableName("test", "test", "table_1"), ImmutableList.of("c1", "c2"));
        expected.putAll(catalogSchemaTableName("test", "test", "table_2"), ImmutableList.of("c1", "c2"));
        assertThat(analyzeSql("SELECT t1.c1, t2.c1, t2.c2 FROM table_1 t1 JOIN table_2 t2 ON t1.c2 = t2.c1", manifest).getCollectedColumns()).isEqualTo(expected);

        expected = HashMultimap.create();
        expected.putAll(catalogSchemaTableName("test", "test", "table_1"), ImmutableList.of("c1", "c2"));
        assertThat(analyzeSql("SELECT t1.c1 FROM table_1 t1 WHERE t1.c2 = 'wah'", manifest).getCollectedColumns()).isEqualTo(expected);
    }

    private Analysis analyzeSql(String sql, Manifest manifest)
    {
        Statement statement = parseSql(sql);
        Analysis analysis = new Analysis(statement);
        analyze(
                analysis,
                statement,
                DEFAULT_SESSION_CONTEXT,
                fromManifest(manifest));
        return analysis;
    }

    @Test
    public void testScope()
    {
        SessionContext sessionContext = SessionContext.builder().setCatalog("test").setSchema("test").build();
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(ImmutableList.of(
                        Model.model("table_1", "SELECT * FROM foo", ImmutableList.of(varcharColumn("c1"), varcharColumn("c2"))),
                        Model.model("table_2", "SELECT * FROM bar", ImmutableList.of(varcharColumn("c1"), varcharColumn("c2"))),
                        Model.model("table_3", "SELECT * FROM foo", ImmutableList.of(varcharColumn("c1"), integerColumn("c2")))))
                .setMetrics(ImmutableList.of(
                        Metric.metric("metric_1", "table_3",
                                ImmutableList.of(varcharColumn("c1")),
                                ImmutableList.of(Column.column("max_c2", WrenTypes.INTEGER, null, false, "max(c2)")))))
                .build();

        Function<String, Scope> analyzeSql = (sql) -> {
            Statement statement = parseSql(sql);
            Analysis analysis = new Analysis(statement);
            return analyze(
                    analysis,
                    statement,
                    sessionContext,
                    fromManifest(manifest));
        };

        Optional<Scope> scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM table_1"));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM table_2"));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM test.test.foo"));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(0);
        assertThat(scope.get().isDataSourceScope()).isTrue();

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM test.foo"));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(0);
        assertThat(scope.get().isDataSourceScope()).isTrue();

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM foo"));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(0);
        assertThat(scope.get().isDataSourceScope()).isTrue();

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM (select * from test.test.foo) table_1"));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");

        scope = Optional.ofNullable(analyzeSql.apply("WITH t1 as (SELECT * FROM (select * from test.test.foo) table_1) select * from t1"));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM table_3"));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");

        scope = Optional.ofNullable(analyzeSql.apply("WITH table_1 as (SELECT * FROM foo), table_3 as (SELECT c1, max(c2) max_c2 FROM table_1)  SELECT * FROM table_3"));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("max_c2");

        scope = Optional.ofNullable(analyzeSql.apply("""
                WITH t1 as (SELECT "c1", "c2" FROM (select * from test.test.foo) table_1) select * from t1
                """));
        Assertions.assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");
    }

    @Test
    public void testScopeWithRelationship()
    {
        SessionContext sessionContext = SessionContext.builder().setCatalog("test").setSchema("test").build();
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(ImmutableList.of(
                        Model.model("table_1", "SELECT * FROM foo", ImmutableList.of(
                                varcharColumn("c1"), varcharColumn("c2"), relationshipColumn("table_2", "table_2", "relationship_1_2"))),
                        Model.model("table_2", "SELECT * FROM bar", ImmutableList.of(varcharColumn("c1"), varcharColumn("c2")))))
                .setRelationships(ImmutableList.of(
                        relationship("relationship_1_2", List.of("table_1", "table_2"), JoinType.ONE_TO_ONE, "table_1.c1 = table_2.c1")))
                .build();

        Statement statement = parseSql("SELECT table_2.c2 FROM table_1 JOIN table_2 ON table_1.c1 = table_2.c1");
        Analysis analysis = new Analysis(statement);
        analyze(analysis, statement, sessionContext, fromManifest(manifest));
        assertThat(analysis.getCollectedColumns().get(catalogSchemaTableName("test", "test", "table_1"))).containsExactly("c1");
        assertThat(analysis.getCollectedColumns().get(catalogSchemaTableName("test", "test", "table_2"))).containsExactly("c1", "c2");
    }

    @Test
    public void testTargetDotAll()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(List.of(
                        model("Orders", "SELECT * FROM tpch.orders",
                                List.of(column("orderkey", "integer", null, false, "o_orderkey"),
                                        column("custkey", "integer", null, false, "o_custkey"),
                                        column("customer", "Customer", "CustomerOrders", false),
                                        calculatedColumn("customer_name", "varchar", "customer.name")),
                                "orderkey"),
                        model("Customer", "SELECT * FROM tpch.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey"),
                                        column("name", "varchar", null, false, "c_name")))))
                .setRelationships(List.of(relationship("CustomerOrders", List.of("Customer", "Orders"), JoinType.ONE_TO_MANY, "Customer.custkey = Orders.custkey")))
                .build();
        List<String> testSqls = ImmutableList.of(
                "SELECT Orders.* FROM Orders",
                "SELECT o.* FROM Orders AS o",
                "SELECT o.* FROM Orders AS o JOIN Customer AS c ON o.custkey = c.custkey");
        for (String sql : testSqls) {
            Statement statement = parseSql(sql);
            Analysis analysis = new Analysis(statement);
            analyze(analysis, statement, DEFAULT_SESSION_CONTEXT, fromManifest(manifest));
            assertThat(analysis.getCollectedColumns().get(catalogSchemaTableName("test", "test", "Orders"))).containsExactly("custkey", "orderkey", "customer");
        }
    }

    private static Column varcharColumn(String name)
    {
        return Column.column(name, "VARCHAR", null, false, null);
    }

    private static Column integerColumn(String name)
    {
        return Column.column(name, "INTEGER", null, false, null);
    }
}
