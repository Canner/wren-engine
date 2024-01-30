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

package io.accio.base.sqlrewrite.analyzer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.SessionContext;
import io.accio.base.dto.Manifest;
import io.accio.base.sqlrewrite.AbstractTestFramework;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.accio.base.AccioMDL.EMPTY;
import static io.accio.base.AccioMDL.fromManifest;
import static io.accio.base.AccioTypes.DATE;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Column.varcharColumn;
import static io.accio.base.dto.Model.model;
import static io.accio.base.sqlrewrite.analyzer.Analysis.SimplePredicate;
import static io.accio.base.sqlrewrite.analyzer.StatementAnalyzer.analyze;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatementAnalyzer
        extends AbstractTestFramework
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final SessionContext DEFAULT_SESSION_CONTEXT =
            SessionContext.builder().setCatalog("test").setSchema("test").build();

    @Test
    public void testValues()
    {
        SessionContext sessionContext = SessionContext.builder().build();
        Statement statement = SQL_PARSER.createStatement("VALUES(1, 'a')", new ParsingOptions(AS_DECIMAL));
        Analysis analysis = new Analysis(statement);
        analyze(analysis, statement, sessionContext, EMPTY);

        statement = SQL_PARSER.createStatement("SELECT * FROM (VALUES(1, 'a'))", new ParsingOptions(AS_DECIMAL));
        analysis = new Analysis(statement);
        analyze(analysis, statement, sessionContext, EMPTY);
    }

    @Test
    public void testGetTableWithoutWithTable()
    {
        SessionContext sessionContext = SessionContext.builder().setCatalog("test").setSchema("test").build();
        Statement statement = SQL_PARSER.createStatement("WITH a AS (SELECT * FROM People) SELECT * FROM a", new ParsingOptions(AS_DECIMAL));
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
                        model("table_1", "SELECT * FROM foo", ImmutableList.of(varcharColumn("c1"), varcharColumn("c2"))),
                        model("table_2", "SELECT * FROM bar", ImmutableList.of(varcharColumn("c1"), varcharColumn("c2")))))
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
    }

    @Test
    public void testGetSimplePredicates()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(ImmutableList.of(
                        model("table_1", "SELECT * FROM foo", ImmutableList.of(varcharColumn("c1"), column("c2", INTEGER, null, true))),
                        model("table_2", "SELECT * FROM bar", ImmutableList.of(varcharColumn("c1"), column("c2", DATE, null, true)))))
                .build();

        CatalogSchemaTableName t1 = new CatalogSchemaTableName("test", "test", "table_1");
        CatalogSchemaTableName t2 = new CatalogSchemaTableName("test", "test", "table_2");
        assertThat(analyzeSql("SELECT t1.c1, t2.c1, t2.c2 FROM table_1 t1 JOIN table_2 t2 ON t1.c2 = t2.c1\n" +
                "WHERE t1.c1 = 'foo' AND t1.c2 >= 123 OR t2.c1 != 'bar' OR t2.c2 < DATE '2020-01-01' AND t1.c1 != t2.c1", manifest).getSimplePredicates())
                .containsExactlyInAnyOrderElementsOf(List.of(
                        new SimplePredicate(t1, "c1", ComparisonExpression.Operator.EQUAL, new StringLiteral("foo")),
                        new SimplePredicate(t1, "c1", ComparisonExpression.Operator.NOT_EQUAL, DereferenceExpression.from(QualifiedName.of("t2", "c1"))),
                        new SimplePredicate(t1, "c2", ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, new LongLiteral("123")),
                        new SimplePredicate(t2, "c1", ComparisonExpression.Operator.NOT_EQUAL, new StringLiteral("bar")),
                        new SimplePredicate(t2, "c2", ComparisonExpression.Operator.LESS_THAN, new GenericLiteral("DATE", "2020-01-01"))));
    }

    private Analysis analyzeSql(String sql, Manifest manifest)
    {
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
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
                        model("table_1", "SELECT * FROM foo", ImmutableList.of(varcharColumn("c1"), varcharColumn("c2"))),
                        model("table_2", "SELECT * FROM bar", ImmutableList.of(varcharColumn("c1"), varcharColumn("c2")))))
                .build();

        Function<String, Scope> analyzeSql = (sql) -> {
            Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
            Analysis analysis = new Analysis(statement);
            return analyze(
                    analysis,
                    statement,
                    sessionContext,
                    fromManifest(manifest));
        };

        Optional<Scope> scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM table_1"));
        assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM table_2"));
        assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM test.test.foo"));
        assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(0);
        assertThat(scope.get().isDataSourceScope()).isTrue();

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM test.foo"));
        assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(0);
        assertThat(scope.get().isDataSourceScope()).isTrue();

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM foo"));
        assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(0);
        assertThat(scope.get().isDataSourceScope()).isTrue();

        scope = Optional.ofNullable(analyzeSql.apply("SELECT * FROM (select * from test.test.foo) table_1"));
        assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");

        scope = Optional.ofNullable(analyzeSql.apply("WITH t1 as (SELECT * FROM (select * from test.test.foo) table_1) select * from t1"));
        assertThat(scope).isPresent();
        assertThat(scope.get().getRelationType().getFields()).hasSize(2);
        assertThat(scope.get().getRelationType().getFields().get(0).getName().get()).isEqualTo("c1");
        assertThat(scope.get().getRelationType().getFields().get(1).getName().get()).isEqualTo("c2");
    }
}
