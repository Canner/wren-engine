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

import io.graphmdl.analyzer.Analysis;
import io.graphmdl.analyzer.StatementAnalyzer;
import io.graphmdl.base.GraphML;
import io.graphmdl.base.GraphMLTypes;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.base.dto.Model;
import io.graphmdl.testing.AbstractTestFramework;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Statement;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.graphmdl.ModelSqlRewrite.MODEL_SQL_REWRITE;
import static io.graphmdl.base.dto.Column.column;
import static io.graphmdl.base.dto.Manifest.manifest;
import static io.graphmdl.base.dto.Relationship.relationship;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestRelationshipAccessing
        extends AbstractTestFramework
{
    @Language("SQL")
    private static final String MODEL_CTE = "" +
            "  Book AS (\n" +
            "     SELECT\n" +
            "        bookId,\n" +
            "        name,\n" +
            "        'relationship<BookPeople>' as author,\n" +
            "        authorId\n" +
            "     FROM (\n" +
            "        SELECT *\n" +
            "        FROM (\n" +
            "           VALUES\n" +
            "           (1, 'book1', 1),\n" +
            "           (2, 'book2', 2),\n" +
            "           (3, 'book3', 3)\n" +
            "        ) Book(bookId, name, authorId)\n" +
            "     )\n" +
            "  ),\n" +
            "  People AS (\n" +
            "   SELECT\n" +
            "     userId,\n" +
            "     name,\n" +
            "     'relationship<BookPeople>' AS book\n" +
            "   FROM\n" +
            "     (\n" +
            "      SELECT *\n" +
            "      FROM\n" +
            "        (\n" +
            "           VALUES\n" +
            "           (1, 'user1'),\n" +
            "           (2, 'user2'),\n" +
            "           (3, 'user3')\n" +
            "        ) People (userId, name)\n" +
            "     )\n" +
            "  )\n";

    @Language("SQL")
    private static final String EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES = "" +
            "WITH\n" + MODEL_CTE + ",\n" +
            "  ${Book.author} (userId, name, book) AS (\n" +
            "   SELECT\n" +
            "     r.userId\n" +
            "   , r.name\n" +
            "   , r.book\n" +
            "   FROM\n" +
            "     (Book l\n" +
            "   LEFT JOIN People r ON (l.authorId = r.userId))\n" +
            ") \n" +
            ", ${Book.author.book} (bookId, name, author, authorId) AS (\n" +
            "   SELECT\n" +
            "     r.bookId\n" +
            "   , r.name\n" +
            "   , r.author\n" +
            "   , r.authorId\n" +
            "   FROM\n" +
            "     (${Book.author} l\n" +
            "   LEFT JOIN Book r ON (l.userId = r.authorId))\n" +
            ") \n" +
            ", ${Book.author.book.author} (userId, name, book) AS (\n" +
            "   SELECT\n" +
            "     r.userId\n" +
            "   , r.name\n" +
            "   , r.book\n" +
            "   FROM\n" +
            "     (${Book.author.book} l\n" +
            "   LEFT JOIN People r ON (l.authorId = r.userId))\n" +
            ")";
    private static final SqlParser SQL_PARSER = new SqlParser();

    private final GraphML graphML;

    public TestRelationshipAccessing()
    {
        graphML = GraphML.fromManifest(manifest(
                List.of(Model.model("Book",
                                "select * from (values (1, 'book1', 1), (2, 'book2', 2), (3, 'book3', 3)) Book(bookId, name, authorId)",
                                List.of(
                                        column("bookId", GraphMLTypes.INTEGER, null, true),
                                        column("name", GraphMLTypes.VARCHAR, null, true),
                                        column("author", "People", "BookPeople", true),
                                        column("authorId", GraphMLTypes.INTEGER, null, true)),
                                "bookId"),
                        Model.model("People",
                                "select * from (values (1, 'user1'), (2, 'user2'), (3, 'user3')) People(userId, name)",
                                List.of(
                                        column("userId", GraphMLTypes.INTEGER, null, true),
                                        column("name", GraphMLTypes.VARCHAR, null, true),

                                        column("book", "Book", "BookPeople", true)),
                                "userId")),
                List.of(relationship("BookPeople", List.of("Book", "People"), JoinType.ONE_TO_ONE, "Book.authorId  = People.userId")),
                List.of(),
                List.of()));
    }

    @DataProvider
    public Object[][] relationshipAccessCases()
    {
        return new Object[][] {
                // TODO: enable this test
//                {"select c1.s1.Book.author.book.author.name,\n" +
//                        "s1.Book.author.book.author.name,\n" +
//                        "Book.author.book.author.name\n" +
//                        "from c1.s1.Book",
//                        EXPECTED_WITH_QUERIES +
//                                "SELECT ${Book.author.book.author}.name, ${Book.author.book.author}.name, ${Book.author.book.author}.name\n" +
//                                "FROM\n" +
//                                "  c1.s1.Book\n" +
//                                ", ${Book.author}\n" +
//                                ", ${Book.author.book}\n" +
//                                ", ${Book.author.book.author}\n"},
                {"select author.book.author.name,\n" +
                        "author.book.name,\n" +
                        "author.name\n" +
                        "from Book",
                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
                                "SELECT\n" +
                                "  ${Book.author.book.author}.name\n" +
                                ", ${Book.author.book}.name\n" +
                                ", ${Book.author}.name\n" +
                                "FROM\n" +
                                "  (((Book\n" +
                                "LEFT JOIN ${Book.author} ON (Book.authorId = ${Book.author}.userId))\n" +
                                "LEFT JOIN ${Book.author.book} ON (Book.bookId = ${Book.author.book}.bookId))\n" +
                                "LEFT JOIN ${Book.author.book.author} ON (Book.authorId = ${Book.author.book.author}.userId))",
                        true},
                {"select name from Book where author.book.author.name = 'jax'",
                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
                                "SELECT name\n" +
                                "FROM\n" +
                                "  (Book\n" +
                                "LEFT JOIN ${Book.author.book.author} ON (Book.authorId = ${Book.author.book.author}.userId))\n" +
                                "WHERE (${Book.author.book.author}.name = 'jax')",
                        false},
                {"select name, author.book.author.name from Book group by author.book.author.name having author.book.name = 'destiny'",
                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
                                "SELECT\n" +
                                "  name\n" +
                                ", ${Book.author.book.author}.name\n" +
                                "FROM\n" +
                                "  ((Book\n" +
                                "LEFT JOIN ${Book.author.book} ON (Book.bookId = ${Book.author.book}.bookId))\n" +
                                "LEFT JOIN ${Book.author.book.author} ON (Book.authorId = ${Book.author.book.author}.userId))\n" +
                                "GROUP BY ${Book.author.book.author}.name\n" +
                                "HAVING (${Book.author.book}.name = 'destiny')",
                        false},
                {"select name, author.book.author.name from Book order by author.book.author.name",
                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
                                "SELECT\n" +
                                "  name\n" +
                                ", ${Book.author.book.author}.name\n" +
                                "FROM\n" +
                                "  (Book\n" +
                                "LEFT JOIN ${Book.author.book.author} ON (Book.authorId = ${Book.author.book.author}.userId))\n" +
                                "ORDER BY ${Book.author.book.author}.name ASC",
                        false},
                {"select a.* from (select name, author.book.author.name from Book order by author.book.author.name) a",
                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
                                "SELECT a.*\n" +
                                "FROM\n" +
                                "  (\n" +
                                "   SELECT\n" +
                                "     name\n" +
                                "   , ${Book.author.book.author}.name\n" +
                                "   FROM\n" +
                                "     (Book\n" +
                                "   LEFT JOIN ${Book.author.book.author} ON (Book.authorId = ${Book.author.book.author}.userId))\n" +
                                "   ORDER BY ${Book.author.book.author}.name ASC\n" +
                                ")  a",
                        false},
                // TODO: enable this test and find a way to reorder queries in with-clause
//                {"with a as (select b.* from (select name, author.book.author.name from Book order by author.book.author.name) b)\n" +
//                        "select * from a", "" // TODO fill expected sql
//                },
                // test the reverse relationship accessing
                {"select book.author.book.name, book.author.name, book.name from People", "" +
                        "WITH\n" + MODEL_CTE + ",\n" +
                        "  ${People.book} (bookId, name, author, authorId) AS (\n" +
                        "   SELECT\n" +
                        "     r.bookId\n" +
                        "   , r.name\n" +
                        "   , r.author\n" +
                        "   , r.authorId\n" +
                        "   FROM\n" +
                        "     (People l\n" +
                        "   LEFT JOIN Book r ON (l.userId = r.authorId))\n" +
                        ") \n" +
                        ", ${People.book.author} (userId, name, book) AS (\n" +
                        "   SELECT\n" +
                        "     r.userId\n" +
                        "   , r.name\n" +
                        "   , r.book\n" +
                        "   FROM\n" +
                        "     (${People.book} l\n" +
                        "   LEFT JOIN People r ON (l.authorId = r.userId))\n" +
                        ") \n" +
                        ", ${People.book.author.book} (bookId, name, author, authorId) AS (\n" +
                        "   SELECT\n" +
                        "     r.bookId\n" +
                        "   , r.name\n" +
                        "   , r.author\n" +
                        "   , r.authorId\n" +
                        "   FROM\n" +
                        "     (${People.book.author} l\n" +
                        "   LEFT JOIN Book r ON (l.userId = r.authorId))\n" +
                        ") \n" +
                        "SELECT\n" +
                        "  ${People.book.author.book}.name\n" +
                        ", ${People.book.author}.name\n" +
                        ", ${People.book}.name\n" +
                        "FROM\n" +
                        "  (((People\n" +
                        "LEFT JOIN ${People.book.author} ON (People.userId = ${People.book.author}.userId))\n" +
                        "LEFT JOIN ${People.book.author.book} ON (People.userId = ${People.book.author.book}.authorId))\n" +
                        "LEFT JOIN ${People.book} ON (People.userId = ${People.book}.authorId))",
                        true},
        };
    }

    @Test(dataProvider = "relationshipAccessCases")
    public void testRelationshipAccessingRewrite(String original, String expected, boolean enableH2Assertion)
    {
        Statement statement = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        RelationshipCteGenerator generator = new RelationshipCteGenerator(graphML);
        Analysis analysis = StatementAnalyzer.analyze(statement, graphML, generator);

        Map<String, String> replaceMap = new HashMap<>();
        replaceMap.put("Book.author", generator.getNameMapping().get("Book.author"));
        replaceMap.put("Book.author.book", generator.getNameMapping().get("Book.author.book"));
        replaceMap.put("Book.author.book.author", generator.getNameMapping().get("Book.author.book.author"));
        replaceMap.put("People.book", generator.getNameMapping().get("People.book"));
        replaceMap.put("People.book.author", generator.getNameMapping().get("People.book.author"));
        replaceMap.put("People.book.author.book", generator.getNameMapping().get("People.book.author.book"));

        Node rewrittenStatement = statement;
        for (GraphMLRule rule : List.of(MODEL_SQL_REWRITE, RelationshipRewrite.RELATIONSHIP_REWRITE)) {
            rewrittenStatement = rule.apply(rewrittenStatement, analysis, graphML);
        }

        Statement expectedResult = SQL_PARSER.createStatement(new StrSubstitutor(replaceMap).replace(expected), new ParsingOptions(AS_DECIMAL));
        String actualSql = SqlFormatter.formatSql(rewrittenStatement);
        assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedResult));
        // TODO: remove this flag, disabled h2 assertion due to ambiguous column name
        if (enableH2Assertion) {
            assertThatNoException()
                    .describedAs(format("actual sql: %s is invalid", actualSql))
                    .isThrownBy(() -> query(actualSql));
        }
    }

    @DataProvider
    public Object[][] notRewritten()
    {
        return new Object[][] {
                {"SELECT col_1 FROM foo"},
                {"SELECT foo.col_1 FROM foo"},
                {"SELECT col_1.a FROM foo"},
                {"WITH foo AS (SELECT 1 AS col_1) SELECT col_1 FROM foo"},
        };
    }

    @Test(dataProvider = "notRewritten")
    public void testNotRewritten(String sql)
    {
        String rewrittenSql = GraphMLPlanner.rewrite(sql, graphML, List.of(MODEL_SQL_REWRITE, RelationshipRewrite.RELATIONSHIP_REWRITE));
        Statement expectedResult = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        assertThat(rewrittenSql).isEqualTo(SqlFormatter.formatSql(expectedResult));
    }

    @Test
    public void testRelationshipOutsideQuery()
    {
        // this is invalid since we don't allow access to relationship field outside the sub-query
        // hence this sql shouldn't be rewritten
        String actualSql = "SELECT a.name, a.author.book.author.name from (SELECT * FROM Book) a";
        String expectedSql = format("WITH Book AS (%s) SELECT a.name, a.author.book.author.name from (SELECT * FROM Book) a",
                Utils.getModelSql(graphML.getModel("Book").orElseThrow()));

        String rewrittenSql = GraphMLPlanner.rewrite(actualSql, graphML, List.of(MODEL_SQL_REWRITE, RelationshipRewrite.RELATIONSHIP_REWRITE));
        Statement expectedResult = SQL_PARSER.createStatement(expectedSql, new ParsingOptions(AS_DECIMAL));
        assertThat(rewrittenSql).isEqualTo(SqlFormatter.formatSql(expectedResult));
    }
}
