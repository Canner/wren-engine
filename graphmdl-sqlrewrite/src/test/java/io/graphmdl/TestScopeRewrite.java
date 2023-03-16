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

package io.graphmdl;

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.GraphMDLTypes;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.base.dto.Model;
import io.graphmdl.testing.AbstractTestFramework;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.graphmdl.base.dto.Column.column;
import static io.graphmdl.base.dto.Relationship.relationship;
import static io.graphmdl.sqlrewrite.ScopeRewrite.SCOPE_REWRITE;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestScopeRewrite
        extends AbstractTestFramework
{
    private final GraphMDL graphMDL;
    private static final SqlParser SQL_PARSER = new SqlParser();

    public TestScopeRewrite()
    {
        graphMDL = GraphMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("Book",
                                "select * from (values (1, 'book1', 1), (2, 'book2', 2), (3, 'book3', 3)) Book(bookId, name, authorId)",
                                List.of(
                                        column("bookId", GraphMDLTypes.INTEGER, null, true),
                                        column("name", GraphMDLTypes.VARCHAR, null, true),
                                        column("author", "People", "BookPeople", true),
                                        column("authorId", GraphMDLTypes.INTEGER, null, true)),
                                "bookId"),
                        Model.model("People",
                                "select * from (values (1, 'user1'), (2, 'user2'), (3, 'user3')) People(userId, name)",
                                List.of(
                                        column("userId", GraphMDLTypes.INTEGER, null, true),
                                        column("name", GraphMDLTypes.VARCHAR, null, true),
                                        column("book", "Book", "BookPeople", true)),
                                "userId")))
                .setRelationships(List.of(relationship("BookPeople", List.of("Book", "People"), JoinType.ONE_TO_ONE, "Book.authorId  = People.userId")))
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
    public Object[][] notRewritten()
    {
        return new Object[][] {
                {"SELECT Book.name FROM Book"},
                {"SELECT test.Book.name FROM Book"},
                {"SELECT graphmdl.test.Book.name FROM Book"},
                {"SELECT name FROM NotBelongToGraphMDL"},
                {"SELECT Book.author.book.name FROM Book"},
                {"SELECT test.Book.author.book.name FROM Book"},
                {"SELECT graphmdl.test.Book.author.book.name FROM Book"},
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
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        return SqlFormatter.formatSql(SCOPE_REWRITE.rewrite(statement, graphMDL));
    }
}
