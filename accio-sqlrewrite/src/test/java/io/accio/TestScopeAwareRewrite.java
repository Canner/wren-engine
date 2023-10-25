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

package io.accio;

import io.accio.base.AccioMDL;
import io.accio.base.AccioTypes;
import io.accio.base.SessionContext;
import io.accio.base.dto.JoinType;
import io.accio.base.dto.Model;
import io.accio.sqlrewrite.ScopeAwareRewrite;
import io.accio.testing.AbstractTestFramework;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.sqlrewrite.ScopeAwareRewrite.SCOPE_AWARE_REWRITE;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestScopeAwareRewrite
        extends AbstractTestFramework
{
    private final AccioMDL accioMDL;
    private static final SqlParser SQL_PARSER = new SqlParser();

    public TestScopeAwareRewrite()
    {
        accioMDL = AccioMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("Book",
                                "select * from (values (1, 'book1', 1), (2, 'book2', 2), (3, 'book3', 3)) Book(bookId, name, authorId)",
                                List.of(
                                        column("bookId", AccioTypes.INTEGER, null, true),
                                        column("name", AccioTypes.VARCHAR, null, true),
                                        column("author", "People", "BookPeople", true),
                                        column("authorId", AccioTypes.INTEGER, null, true)),
                                "bookId"),
                        Model.model("People",
                                "select * from (values (1, 'user1'), (2, 'user2'), (3, 'user3')) People(userId, name)",
                                List.of(
                                        column("userId", AccioTypes.INTEGER, null, true),
                                        column("name", AccioTypes.VARCHAR, null, true),
                                        column("book", "Book", "BookPeople", true),
                                        column("items", "Item", "ItemPeople", true)),
                                "userId"),
                        Model.model("Item",
                                "select * from (values (1, 'item1', 1), (2, 'item2', 2), (3, 'item3', 1), (4, 'item4', 3) Item(itemId, name, userId)",
                                List.of(
                                        column("itemId", AccioTypes.INTEGER, null, true),
                                        column("price", AccioTypes.INTEGER, null, true),
                                        column("user", "People", "ItemPeople", true)),
                                "orderId")))
                .setRelationships(List.of(
                        relationship("BookPeople", List.of("Book", "People"), JoinType.ONE_TO_ONE, "Book.authorId  = People.userId"),
                        relationship("ItemPeople", List.of("Item", "People"), JoinType.MANY_TO_ONE, "Item.userId  = People.userId")))
                .setMetrics(List.of(
                        metric(
                                "AuthorBookCount",
                                "Book",
                                List.of(column("authorId", VARCHAR, null, true)),
                                List.of(column("count", INTEGER, null, true, "sum(*)")),
                                List.of())))
                .build());
    }

    @DataProvider
    public Object[][] scopeRewrite()
    {
        return new Object[][] {
                {"SELECT name FROM Book", "SELECT Book.name FROM Book"},
                {"SELECT name FROM Book b", "SELECT b.name FROM Book b"},
                {"SELECT * FROM Book WHERE name = 'canner'", "SELECT * FROM Book WHERE Book.name = 'canner'"},
                {"SELECT * FROM Book b WHERE name = 'canner'", "SELECT * FROM Book b WHERE b.name = 'canner'"},
                {"SELECT author.book.name FROM Book", "SELECT Book.author.book.name FROM Book"},
                {"SELECT author.book.name FROM Book b", "SELECT b.author.book.name FROM Book b"},
                {"SELECT name, count(*) FROM Book b GROUP BY name", "SELECT b.name, count(*) FROM Book b GROUP BY b.name"},
                {"SELECT b.name, p.name, book FROM Book b JOIN People p ON authorId = userId", "SELECT b.name, p.name, p.book FROM Book b JOIN People p ON b.authorId = p.userId"},
                {"SELECT user.items[1].price FROM Item", "SELECT Item.user.items[1].price FROM Item"},
                {"SELECT name FROM accio.test.Book", "SELECT Book.name FROM accio.test.Book"},
                {"SELECT name FROM test.Book", "SELECT Book.name FROM test.Book"},
                {"SELECT name FROM (SELECT * FROM Book) b", "SELECT name FROM (SELECT * FROM Book) b"},
                {"SELECT name FROM (SELECT name FROM Book) b", "SELECT name FROM (SELECT Book.name FROM Book) b"},
                {"WITH b AS (SELECT name, author FROM Book) SELECT author FROM b", "WITH b AS (SELECT Book.name, Book.author FROM Book) SELECT author FROM b"},
                {"WITH b AS (SELECT o_clerk, author FROM Book) SELECT author FROM b", "WITH b AS (SELECT o_clerk, Book.author FROM Book) SELECT author FROM b"},
                {"SELECT concat(name, '12') FROM test.Book", "SELECT concat(Book.name, '12') FROM test.Book"},
                {"SELECT concat(name, '12') = '123' FROM test.Book", "SELECT concat(Book.name, '12') = '123' FROM test.Book"},
                {"SELECT concat(name, '12') + 123 FROM test.Book", "SELECT concat(Book.name, '12') + 123 FROM test.Book"},
                {"SELECT accio.test.Book.author.book.name FROM Book", "SELECT Book.author.book.name FROM Book"},
                {"SELECT accio.test.Book.author.books[1].name FROM Book", "SELECT Book.author.books[1].name FROM Book"},
                {"SELECT test.Book.author.book.name FROM Book", "SELECT Book.author.book.name FROM Book"},
                {"SELECT test.Book.author.books[1].name FROM Book", "SELECT Book.author.books[1].name FROM Book"},
                {"SELECT accio.test.AuthorBookCount.authorId FROM AuthorBookCount", "SELECT AuthorBookCount.authorId FROM AuthorBookCount"},
                {"SELECT test.AuthorBookCount.authorId FROM AuthorBookCount", "SELECT AuthorBookCount.authorId FROM AuthorBookCount"},
                {"SELECT authorId FROM AuthorBookCount", "SELECT AuthorBookCount.authorId FROM AuthorBookCount"},
        };
    }

    @Test(dataProvider = "scopeRewrite")
    public void testScopeRewriter(String original, String expected)
    {
        Statement expectedState = SQL_PARSER.createStatement(expected, new ParsingOptions(AS_DECIMAL));
        String actualSql = rewrite(original);
        Assertions.assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedState));
    }

    @DataProvider
    public Object[][] wrongSessionContextNoRewrite()
    {
        return new Object[][] {
                {"SELECT name FROM Book"},
                {"SELECT name FROM test.Book"},
                {"SELECT name FROM fake1.test.Book"}
        };
    }

    @Test(dataProvider = "wrongSessionContextNoRewrite")
    public void testScopeRewriterWithWrongSessionContextNoRewrite(String original)
    {
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog("wrongCatalog")
                .setSchema("wrongSchema")
                .build();

        Statement expectedState = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        String actualSql = rewrite(original, sessionContext);
        Assertions.assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedState));
    }

    @Test
    public void testScopeRewriterWithWrongSessionContextRewrite()
    {
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog("wrongCatalog")
                .setSchema("wrongSchema")
                .build();
        String sql = "SELECT name FROM accio.test.Book";
        String expected = "SELECT Book.name FROM accio.test.Book";

        Statement expectedState = SQL_PARSER.createStatement(expected, new ParsingOptions(AS_DECIMAL));
        String actualSql = rewrite(sql, sessionContext);
        Assertions.assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedState));
    }

    @DataProvider
    public Object[][] notRewritten()
    {
        return new Object[][] {
                {"SELECT Book.name FROM Book"},
                {"SELECT name FROM NotBelongToAccio"},
                {"SELECT Book.author.book.name FROM Book"},
                {"SELECT name FROM fake1.fake2.Book"},
                {"SELECT name FROM fake2.Book"},
                {"WITH b AS (SELECT * FROM Book) SELECT author FROM b"},
                {"SELECT notfound FROM b"},
                {"SELECT Book.author.books[1].name FROM Book"},
                {"SELECT AuthorBookCount.authorId FROM AuthorBookCount"},
                {"SELECT Book.author.book.name FROM Book"},
                {"SELECT fakecatalog.fakeschema.Book.author.book.name FROM Book"},
                {"SELECT fakeschema.Book.author.book.name FROM Book"},
        };
    }

    @Test(dataProvider = "notRewritten")
    public void testNotRewritten(String sql)
    {
        String rewrittenSql = rewrite(sql);
        Statement expectedResult = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        assertThat(rewrittenSql).isEqualTo(SqlFormatter.formatSql(expectedResult));
    }

    @Test
    public void testDetectAmbiguous()
    {
        String sql = "SELECT name, book FROM Book b JOIN People p ON authorId = userId";
        Assertions.assertThatThrownBy(() -> rewrite(sql))
                .hasMessage("Ambiguous column name: name");
    }

    private String rewrite(String sql)
    {
        return rewrite(sql, DEFAULT_SESSION_CONTEXT);
    }

    private String rewrite(String sql, SessionContext sessionContext)
    {
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        return SqlFormatter.formatSql(SCOPE_AWARE_REWRITE.rewrite(statement, accioMDL, sessionContext));
    }

    @DataProvider
    public Object[][] addPrefix()
    {
        return new Object[][] {
                {"author.book.name", "Book.author.book.name"},
                {"author.books[1].name", "Book.author.books[1].name"},
        };
    }

    @Test(dataProvider = "addPrefix")
    public void testAddPrefix(String source, String expected)
    {
        Expression expression = getSelectItem(String.format("SELECT %s FROM Book", source));
        Expression node = ScopeAwareRewrite.addPrefix(expression, new Identifier("Book"));
        assertThat(node.toString()).isEqualTo(expected);
    }

    private Expression getSelectItem(String sql)
    {
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        return ((SingleColumn) ((QuerySpecification) ((Query) statement).getQueryBody()).getSelect().getSelectItems().get(0)).getExpression();
    }
}
