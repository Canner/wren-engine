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

package io.graphmdl.sqlrewrite;

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.GraphMDLTypes;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.Relationship;
import io.graphmdl.sqlrewrite.analyzer.Analysis;
import io.graphmdl.sqlrewrite.analyzer.StatementAnalyzer;
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

import static io.graphmdl.base.dto.Column.column;
import static io.graphmdl.base.dto.Relationship.SortKey.sortKey;
import static io.graphmdl.base.dto.Relationship.relationship;
import static io.graphmdl.sqlrewrite.GraphMDLSqlRewrite.GRAPHMDL_SQL_REWRITE;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestRelationshipAccessing
        extends AbstractTestFramework
{
    @Language("SQL")
    private static final String ONE_TO_ONE_MODEL_CTE = "" +
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
    private static final String ONE_TO_MANY_MODEL_CTE = "" +
            "  Book AS (\n" +
            "     SELECT\n" +
            "        bookId,\n" +
            "        name,\n" +
            "        'relationship<PeopleBook>' as author,\n" +
            "        'relationship<BookPeople>' as author_reverse,\n" +
            "        authorId\n" +
            "     FROM (\n" +
            "        SELECT *\n" +
            "        FROM (\n" +
            "           VALUES\n" +
            "           (1, 'book1', 1),\n" +
            "           (2, 'book2', 2),\n" +
            "           (3, 'book3', 1)\n" +
            "        ) Book(bookId, name, authorId)\n" +
            "     )\n" +
            "  ),\n" +
            "  People AS (\n" +
            "   SELECT\n" +
            "     userId,\n" +
            "     name,\n" +
            // TODO: Remove this field. In ONE_TO_MANY relationship, user can access it directly.
            "     'relationship<PeopleBook>' AS books\n" +
            ",    'relationship<PeopleBookOrderByName>' sorted_books\n" +
            "   FROM\n" +
            "     (\n" +
            "      SELECT *\n" +
            "      FROM\n" +
            "        (\n" +
            "           VALUES\n" +
            "           (1, 'user1'),\n" +
            "           (2, 'user2')\n" +
            "        ) People (userId, name)\n" +
            "     )\n" +
            "  )\n";

    @Language("SQL")
    private static final String EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES = "" +
            "WITH\n" + ONE_TO_ONE_MODEL_CTE + ",\n" +
            "  ${Book.author} (userId, name, book) AS (\n" +
            "   SELECT\n" +
            "     t.userId\n" +
            "   , t.name\n" +
            "   , t.book\n" +
            "   FROM\n" +
            "     (Book s\n" +
            "   LEFT JOIN People t ON (s.authorId = t.userId))\n" +
            ") \n" +
            ", ${Book.author.book} (bookId, name, author, authorId) AS (\n" +
            "   SELECT\n" +
            "     t.bookId\n" +
            "   , t.name\n" +
            "   , t.author\n" +
            "   , t.authorId\n" +
            "   FROM\n" +
            "     (${Book.author} s\n" +
            "   LEFT JOIN Book t ON (s.userId = t.authorId))\n" +
            ") \n" +
            ", ${Book.author.book.author} (userId, name, book) AS (\n" +
            "   SELECT\n" +
            "     t.userId\n" +
            "   , t.name\n" +
            "   , t.book\n" +
            "   FROM\n" +
            "     (${Book.author.book} s\n" +
            "   LEFT JOIN People t ON (s.authorId = t.userId))\n" +
            ")";
    private static final SqlParser SQL_PARSER = new SqlParser();

    private final GraphMDL oneToOneGraphMDL;
    private final GraphMDL oneToManyGraphMDL;

    public TestRelationshipAccessing()
    {
        oneToOneGraphMDL = GraphMDL.fromManifest(withDefaultCatalogSchema()
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

        oneToManyGraphMDL = GraphMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("Book",
                                "select * from (values (1, 'book1', 1), (2, 'book2', 2), (3, 'book3', 1)) Book(bookId, name, authorId)",
                                List.of(
                                        column("bookId", GraphMDLTypes.INTEGER, null, true),
                                        column("name", GraphMDLTypes.VARCHAR, null, true),
                                        column("author", "People", "PeopleBook", true),
                                        column("author_reverse", "People", "BookPeople", true),
                                        column("authorId", GraphMDLTypes.INTEGER, null, true)),
                                "bookId"),
                        Model.model("People",
                                "select * from (values (1, 'user1'), (2, 'user2')) People(userId, name)",
                                List.of(
                                        column("userId", GraphMDLTypes.INTEGER, null, true),
                                        column("name", GraphMDLTypes.VARCHAR, null, true),
                                        column("books", "Book", "PeopleBook", true),
                                        column("sorted_books", "Book", "PeopleBookOrderByName", true)),
                                "userId")))
                .setRelationships(List.of(
                        relationship("PeopleBook", List.of("People", "Book"), JoinType.ONE_TO_MANY, "People.userId = Book.authorId"),
                        relationship("BookPeople", List.of("Book", "People"), JoinType.MANY_TO_ONE, "Book.authorId = People.userId"),
                        relationship("PeopleBookOrderByName", List.of("People", "Book"), JoinType.ONE_TO_MANY, "People.userId = Book.authorId",
                                List.of(sortKey("name", Relationship.SortKey.Ordering.ASC), sortKey("bookId", Relationship.SortKey.Ordering.DESC)))))
                .build());
    }

    @DataProvider
    public Object[][] oneToOneRelationshipAccessCases()
    {
        return new Object[][] {
                {"SELECT a.author.book.author.name\n" +
                        "FROM Book a",
                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
                                "SELECT ${Book.author.book.author}.name\n" +
                                "FROM\n" +
                                "  (Book a\n" +
                                "LEFT JOIN ${Book.author.book.author} ON (a.authorId = ${Book.author.book.author}.userId))",
                        true},
                {"SELECT a.author.book.author.name, a.author.book.name, a.author.name\n" +
                        "FROM Book a",
                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
                                "SELECT\n" +
                                "  ${Book.author.book.author}.name\n" +
                                ", ${Book.author.book}.name\n" +
                                ", ${Book.author}.name\n" +
                                "FROM\n" +
                                "  (((Book a\n" +
                                "LEFT JOIN ${Book.author} ON (a.authorId = ${Book.author}.userId))\n" +
                                "LEFT JOIN ${Book.author.book} ON (a.bookId = ${Book.author.book}.bookId))\n" +
                                "LEFT JOIN ${Book.author.book.author} ON (a.authorId = ${Book.author.book.author}.userId))",
                        true},
                // TODO: support join models
                // {"SELECT author.book.author.name, book.name\n" +
                //         "FROM Book JOIN People on Book.authorId = People.userId",
                //         "SELECT 1",
                //         true},
                // {"SELECT a.author.book.author.name, b book.name\n" +
                //         "FROM Book a JOIN People b on a.authorId = b.userId",
                //         "SELECT 1",
                //         true},
                {"SELECT graphmdl.test.Book.author.book.author.name,\n" +
                        "test.Book.author.book.author.name,\n" +
                        "Book.author.book.author.name\n" +
                        "FROM graphmdl.test.Book",
                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
                                "SELECT ${Book.author.book.author}.name,\n" +
                                "${Book.author.book.author}.name,\n" +
                                "${Book.author.book.author}.name\n" +
                                "FROM\n" +
                                "  (Book\n" +
                                "LEFT JOIN ${Book.author.book.author} ON (Book.authorId = ${Book.author.book.author}.userId))",
                        true},
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
                {"with a as (select b.* from (select name, author.book.author.name from Book) b)\n" +
                        "select * from a",
                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
                                ", a as (" +
                                "SELECT b.* from (\n" +
                                "   SELECT " +
                                "      name,\n" +
                                "      ${Book.author.book.author}.name\n" +
                                "   FROM " +
                                "      (Book " +
                                "   LEFT JOIN ${Book.author.book.author} ON (Book.authorId = ${Book.author.book.author}.userId))\n" +
                                ") b)\n" +
                                "SELECT * FROM a",
                        false
                },
                // test the reverse relationship accessing
                {"select book.author.book.name, book.author.name, book.name from People", "" +
                        "WITH\n" + ONE_TO_ONE_MODEL_CTE + ",\n" +
                        "  ${People.book} (bookId, name, author, authorId) AS (\n" +
                        "   SELECT\n" +
                        "     t.bookId\n" +
                        "   , t.name\n" +
                        "   , t.author\n" +
                        "   , t.authorId\n" +
                        "   FROM\n" +
                        "     (People s\n" +
                        "   LEFT JOIN Book t ON (s.userId = t.authorId))\n" +
                        ") \n" +
                        ", ${People.book.author} (userId, name, book) AS (\n" +
                        "   SELECT\n" +
                        "     t.userId\n" +
                        "   , t.name\n" +
                        "   , t.book\n" +
                        "   FROM\n" +
                        "     (${People.book} s\n" +
                        "   LEFT JOIN People t ON (s.authorId = t.userId))\n" +
                        ") \n" +
                        ", ${People.book.author.book} (bookId, name, author, authorId) AS (\n" +
                        "   SELECT\n" +
                        "     t.bookId\n" +
                        "   , t.name\n" +
                        "   , t.author\n" +
                        "   , t.authorId\n" +
                        "   FROM\n" +
                        "     (${People.book.author} s\n" +
                        "   LEFT JOIN Book t ON (s.userId = t.authorId))\n" +
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
                {"SELECT a.author\n" +
                        "FROM Book a",
                        "WITH\n" + ONE_TO_ONE_MODEL_CTE + ",\n" +
                                // TODO: better to remove this unused CTE
                                "  ${Book.author} (userId, name, book) AS (\n" +
                                "   SELECT\n" +
                                "     t.userId\n" +
                                "   , t.name\n" +
                                "   , t.book\n" +
                                "   FROM\n" +
                                "     (Book s\n" +
                                "   LEFT JOIN People t ON (s.authorId = t.userId))\n" +
                                ") \n" +
                                "SELECT a.author\n" +
                                "FROM Book a",
                        true},
                {"WITH A as (SELECT b.author.name FROM Book b) SELECT A.name FROM A",
                        "WITH\n" + ONE_TO_ONE_MODEL_CTE + ",\n" +
                                " ${Book.author} (userId, name, book) AS (\n" +
                                "   SELECT\n" +
                                "     t.userId\n" +
                                "   , t.name\n" +
                                "   , t.book\n" +
                                "   FROM\n" +
                                "     (Book s\n" +
                                "   LEFT JOIN People t ON (s.authorId = t.userId))\n" +
                                ") \n" +
                                ", A AS (\n" +
                                "   SELECT ${Book.author}.name\n" +
                                "   FROM\n" +
                                "     (Book b\n" +
                                "   LEFT JOIN ${Book.author} ON (b.authorId = ${Book.author}.userId))\n" +
                                ") \n" +
                                "SELECT A.name\n" +
                                "FROM\n" +
                                "  A", true},
        };
    }

    @Test(dataProvider = "oneToOneRelationshipAccessCases")
    public void testOneToOneRelationshipAccessingRewrite(String original, String expected, boolean enableH2Assertion)
    {
        Statement statement = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        RelationshipCteGenerator generator = new RelationshipCteGenerator(oneToOneGraphMDL);
        Analysis analysis = StatementAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, oneToOneGraphMDL, generator);

        Map<String, String> replaceMap = new HashMap<>();
        replaceMap.put("Book.author", generator.getNameMapping().get("Book.author"));
        replaceMap.put("Book.author.book", generator.getNameMapping().get("Book.author.book"));
        replaceMap.put("Book.author.book.author", generator.getNameMapping().get("Book.author.book.author"));
        replaceMap.put("People.book", generator.getNameMapping().get("People.book"));
        replaceMap.put("People.book.author", generator.getNameMapping().get("People.book.author"));
        replaceMap.put("People.book.author.book", generator.getNameMapping().get("People.book.author.book"));

        Node rewrittenStatement = statement;
        for (GraphMDLRule rule : List.of(GRAPHMDL_SQL_REWRITE)) {
            rewrittenStatement = rule.apply((Statement) rewrittenStatement, DEFAULT_SESSION_CONTEXT, analysis, oneToOneGraphMDL);
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

    @Test
    public void testNotFoundRelationAliased()
    {
        String original = "select b.book.author.book.name from Book a";
        Statement statement = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        RelationshipCteGenerator generator = new RelationshipCteGenerator(oneToOneGraphMDL);
        Analysis analysis = StatementAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, oneToOneGraphMDL, generator);

        Node rewrittenStatement = statement;
        for (GraphMDLRule rule : List.of(GRAPHMDL_SQL_REWRITE)) {
            rewrittenStatement = rule.apply((Statement) rewrittenStatement, DEFAULT_SESSION_CONTEXT, analysis, oneToOneGraphMDL);
        }

        String expected = "WITH\n" +
                "  Book AS (\n" +
                "   SELECT\n" +
                "     bookId\n" +
                "   , name\n" +
                "   , 'relationship<BookPeople>' author\n" +
                "   , authorId\n" +
                "   FROM\n" +
                "     (\n" +
                "      SELECT *\n" +
                "      FROM\n" +
                "        (\n" +
                " VALUES \n" +
                "           ROW (1, 'book1', 1)\n" +
                "         , ROW (2, 'book2', 2)\n" +
                "         , ROW (3, 'book3', 3)\n" +
                "      )  Book (bookId, name, authorId)\n" +
                "   ) \n" +
                ") \n" +
                "SELECT b.book.author.book.name\n" +
                "FROM\n" +
                "  Book a";
        Statement expectedResult = SQL_PARSER.createStatement(expected, new ParsingOptions(AS_DECIMAL));
        @Language("SQL") String actualSql = SqlFormatter.formatSql(rewrittenStatement);
        assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedResult));
        assertThatThrownBy(() -> query(actualSql))
                .hasMessageContaining("Database \"B\" not found;");
    }

    @DataProvider
    public Object[][] oneToManyRelationshipAccessCase()
    {
        return new Object[][] {
                {"SELECT books[1].name FROM People",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                ", ${People.books[1]} (bookId, name, author, author_reverse, authorId) AS (\n" +
                                "   SELECT\n" +
                                "     r.bookId bookId\n" +
                                "   , r.name name\n" +
                                "   , r.author author\n" +
                                "   , r.author_reverse author_reverse\n" +
                                "   , r.authorId authorId\n" +
                                "   FROM\n" +
                                "     (${People.books} l\n" +
                                "   LEFT JOIN Book r ON (l.books[1] = r.bookId))\n" +
                                ") \n" +
                                "SELECT ${People.books[1]}.name\n" +
                                "FROM\n" +
                                "  (People\n" +
                                "LEFT JOIN ${People.books[1]} ON (People.userId = ${People.books[1]}.authorId))", false},
                {"SELECT books[1].author.books[1].name FROM People",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                ", ${People.books[1]} (bookId, name, author, author_reverse, authorId) AS (\n" +
                                "   SELECT\n" +
                                "     r.bookId bookId\n" +
                                "   , r.name name\n" +
                                "   , r.author author\n" +
                                "   , r.author_reverse author_reverse\n" +
                                "   , r.authorId authorId\n" +
                                "   FROM\n" +
                                "     (${People.books} l\n" +
                                "   LEFT JOIN Book r ON (l.books[1] = r.bookId))\n" +
                                ") \n" +
                                ", ${People.books[1].author} (userId, name, books, sorted_books) AS (\n" +
                                "   SELECT DISTINCT\n" +
                                "     t.userId\n" +
                                "   , t.name\n" +
                                "   , t.books\n" +
                                "   , t.sorted_books\n" +
                                "   FROM\n" +
                                "     (${People.books[1]} s\n" +
                                "   LEFT JOIN People t ON (s.authorId = t.userId))\n" +
                                ") \n" +
                                ", ${People.books[1].author.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (${People.books[1].author} o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                ", ${People.books[1].author.books[1]} (bookId, name, author, author_reverse, authorId) AS (\n" +
                                "   SELECT\n" +
                                "     r.bookId bookId\n" +
                                "   , r.name name\n" +
                                "   , r.author author\n" +
                                "   , r.author_reverse author_reverse\n" +
                                "   , r.authorId authorId\n" +
                                "   FROM\n" +
                                "     (${People.books[1].author.books} l\n" +
                                "   LEFT JOIN Book r ON (l.books[1] = r.bookId))\n" +
                                ") \n" +
                                "SELECT ${People.books[1].author.books[1]} .name\n" +
                                "FROM\n" +
                                "  (People\n" +
                                "LEFT JOIN ${People.books[1].author.books[1]}  ON (People.userId = ${People.books[1].author.books[1]} .authorId))", false},
                {"SELECT cardinality(books) FROM People",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT cardinality(${People.books}.books)\n" +
                                "FROM\n" +
                                "  (People\n" +
                                "LEFT JOIN ${People.books} ON (People.userId = ${People.books}.userId))", false},
                {"SELECT cardinality(People.books) FROM People",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT cardinality(${People.books}.books)\n" +
                                "FROM\n" +
                                "  (People\n" +
                                "LEFT JOIN ${People.books} ON (People.userId = ${People.books}.userId))", false},
                {"SELECT cardinality(author.books) FROM Book",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${Book.author} (userId, name, books, sorted_books) AS (\n" +
                                "   SELECT DISTINCT\n" +
                                "     t.userId\n" +
                                "   , t.name\n" +
                                "   , t.books\n" +
                                "   , t.sorted_books\n" +
                                "   FROM\n" +
                                "     (Book s\n" +
                                "   LEFT JOIN People t ON (s.authorId = t.userId))\n" +
                                ") \n" +
                                ", ${Book.author.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (${Book.author} o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT cardinality(${Book.author.books}.books)\n" +
                                "FROM\n" +
                                "  (Book\n" +
                                "LEFT JOIN ${Book.author.books} ON (Book.authorId = ${Book.author.books}.userId))", false},
                {"SELECT author_reverse.name FROM Book",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${Book.author_reverse} (userId, name, books, sorted_books) AS (\n" +
                                "   SELECT DISTINCT\n" +
                                "     t.userId\n" +
                                "   , t.name\n" +
                                "   , t.books\n" +
                                "   , t.sorted_books\n" +
                                "   FROM\n" +
                                "     (Book s\n" +
                                "   LEFT JOIN People t ON (s.authorId = t.userId))\n" +
                                ") \n" +
                                "SELECT ${Book.author_reverse}.name\n" +
                                "FROM\n" +
                                "  (Book\n" +
                                "LEFT JOIN ${Book.author_reverse} ON (Book.authorId = ${Book.author_reverse}.userId))", false},
                {"SELECT author.name FROM Book",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${Book.author} (userId, name, books, sorted_books) AS (\n" +
                                "   SELECT DISTINCT\n" +
                                "     t.userId\n" +
                                "   , t.name\n" +
                                "   , t.books\n" +
                                "   , t.sorted_books\n" +
                                "   FROM\n" +
                                "     (Book s\n" +
                                "   LEFT JOIN People t ON (s.authorId = t.userId))\n" +
                                ") \n" +
                                "SELECT ${Book.author}.name\n" +
                                "FROM\n" +
                                "  (Book\n" +
                                "LEFT JOIN ${Book.author} ON (Book.authorId = ${Book.author}.userId))", false},
                {"SELECT cardinality(sorted_books) FROM People",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.sorted_books} (userId, sorted_books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.name ASC, m.bookId DESC) filter(WHERE m.bookId IS NOT NULL) sorted_books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT cardinality(${People.sorted_books}.sorted_books)\n" +
                                "FROM\n" +
                                "  (People\n" +
                                "LEFT JOIN ${People.sorted_books} ON (People.userId = ${People.sorted_books}.userId))", false},
                {"SELECT sorted_books[1].name FROM People",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.sorted_books} (userId, sorted_books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.name ASC, m.bookId DESC) filter(WHERE m.bookId IS NOT NULL) sorted_books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                ", ${People.sorted_books[1]} (bookId, name, author, author_reverse, authorId) AS (\n" +
                                "   SELECT\n" +
                                "     r.bookId bookId\n" +
                                "   , r.name name\n" +
                                "   , r.author author\n" +
                                "   , r.author_reverse author_reverse\n" +
                                "   , r.authorId authorId\n" +
                                "   FROM\n" +
                                "     (${People.sorted_books} l\n" +
                                "   LEFT JOIN Book r ON (l.sorted_books[1] = r.bookId))\n" +
                                ") \n" +
                                "SELECT ${People.sorted_books[1]}.name\n" +
                                "FROM\n" +
                                "  (People\n" +
                                "LEFT JOIN ${People.sorted_books[1]} ON (People.userId = ${People.sorted_books[1]}.authorId))", false},
                {"SELECT cardinality(books) FROM People p",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT cardinality(${People.books}.books)\n" +
                                "FROM\n" +
                                "  (People p\n" +
                                "LEFT JOIN ${People.books} ON (p.userId = ${People.books}.userId))", false},
                {"SELECT p.name, cardinality(books) FROM People p",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  p.name\n" +
                                ", cardinality(${People.books}.books)\n" +
                                "FROM\n" +
                                "  (People p\n" +
                                "LEFT JOIN ${People.books} ON (p.userId = ${People.books}.userId))", false},
        };
    }

    @Test(dataProvider = "oneToManyRelationshipAccessCase")
    public void testOneToManyRelationshipAccessingRewrite(String original, String expected, boolean enableH2Assertion)
    {
        Statement statement = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        RelationshipCteGenerator generator = new RelationshipCteGenerator(oneToManyGraphMDL);
        Analysis analysis = StatementAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, oneToManyGraphMDL, generator);

        Node rewrittenStatement = statement;
        for (GraphMDLRule rule : List.of(GRAPHMDL_SQL_REWRITE)) {
            rewrittenStatement = rule.apply((Statement) rewrittenStatement, DEFAULT_SESSION_CONTEXT, analysis, oneToManyGraphMDL);
        }

        Map<String, String> replaceMap = new HashMap<>();
        replaceMap.put("People.books", generator.getNameMapping().get("People.books"));
        replaceMap.put("Book.author", generator.getNameMapping().get("Book.author"));
        replaceMap.put("Book.author.books", generator.getNameMapping().get("Book.author.books"));
        replaceMap.put("Book.author_reverse", generator.getNameMapping().get("Book.author_reverse"));
        replaceMap.put("People.books[1]", generator.getNameMapping().get("People.books[1]"));
        replaceMap.put("People.books[1].author", generator.getNameMapping().get("People.books[1].author"));
        replaceMap.put("People.books[1].author.books", generator.getNameMapping().get("People.books[1].author.books"));
        replaceMap.put("People.books[1].author.books[1]", generator.getNameMapping().get("People.books[1].author.books[1]"));
        replaceMap.put("People.sorted_books", generator.getNameMapping().get("People.sorted_books"));
        replaceMap.put("People.sorted_books[1]", generator.getNameMapping().get("People.sorted_books[1]"));

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
        String rewrittenSql = GraphMDLPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, oneToOneGraphMDL, List.of(GRAPHMDL_SQL_REWRITE));
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
                Utils.getModelSql(oneToOneGraphMDL.getModel("Book").orElseThrow()));

        String rewrittenSql = GraphMDLPlanner.rewrite(actualSql, DEFAULT_SESSION_CONTEXT, oneToOneGraphMDL, List.of(GRAPHMDL_SQL_REWRITE));
        Statement expectedResult = SQL_PARSER.createStatement(expectedSql, new ParsingOptions(AS_DECIMAL));
        assertThat(rewrittenSql).isEqualTo(SqlFormatter.formatSql(expectedResult));
    }

    @DataProvider
    public Object[][] transform()
    {
        return new Object[][] {
                {"select p.name, transform(p.books, book -> book.name) as book_names from People p",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                " ${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                ", ${transform(p.books, (book) -> book.name)} (userId, f1) AS (\n" +
                                "   SELECT\n" +
                                "     s.userId userId\n" +
                                "   , array_agg(t.name ORDER BY t.bookId ASC) filter(WHERE t.name IS NOT NULL) f1\n" +
                                "   FROM\n" +
                                "     ((${People.books} s\n" +
                                "   CROSS JOIN UNNEST(s.books) u (uc))\n" +
                                "   LEFT JOIN Book t ON (u.uc = t.bookId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  p.name\n" +
                                ", ${transform(p.books, (book) -> book.name)}.f1 book_names\n" +
                                "FROM\n" +
                                "  (People p\n" +
                                "LEFT JOIN ${transform(p.books, (book) -> book.name)} ON (p.userId = ${transform(p.books, (book) -> book.name)}.userId))"},
                {"select p.name, transform(p.books, book -> concat(book.name, '_1')) as book_names from People p",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) FILTER (WHERE (m.bookId IS NOT NULL)) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                ", ${transform(p.books, (book) -> concat(book.name, '_1'))} (userId, f1) AS (\n" +
                                "   SELECT\n" +
                                "     s.userId userId\n" +
                                "   , array_agg(concat(t.name, '_1') ORDER BY t.bookId ASC) FILTER (WHERE (concat(t.name, '_1') IS NOT NULL)) f1\n" +
                                "   FROM\n" +
                                "     ((${People.books} s\n" +
                                "   CROSS JOIN UNNEST(s.books) u (uc))\n" +
                                "   LEFT JOIN Book t ON (u.uc = t.bookId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  p.name\n" +
                                ", ${transform(p.books, (book) -> concat(book.name, '_1'))}.f1 book_names\n" +
                                "FROM\n" +
                                "  (People p\n" +
                                "LEFT JOIN ${transform(p.books, (book) -> concat(book.name, '_1'))} ON (p.userId = ${transform(p.books, (book) -> concat(book.name, '_1'))}.userId))"},
        };
    }

    @Test(dataProvider = "transform")
    public void testTransform(String original, String expected)
    {
        Statement statement = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        RelationshipCteGenerator generator = new RelationshipCteGenerator(oneToManyGraphMDL);
        Analysis analysis = StatementAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, oneToManyGraphMDL, generator);

        Statement rewrittenStatement = statement;
        for (GraphMDLRule rule : List.of(GRAPHMDL_SQL_REWRITE)) {
            rewrittenStatement = rule.apply(rewrittenStatement, DEFAULT_SESSION_CONTEXT, analysis, oneToManyGraphMDL);
        }

        Map<String, String> replaceMap = new HashMap<>();
        replaceMap.put("People.books", generator.getNameMapping().get("People.books"));
        replaceMap.put("transform(p.books, (book) -> book.name)", generator.getNameMapping().get("transform(p.books, (book) -> book.name)"));
        replaceMap.put("transform(p.books, (book) -> concat(book.name, '_1'))", generator.getNameMapping().get("transform(p.books, (book) -> concat(book.name, '_1'))"));

        Statement expectedResult = SQL_PARSER.createStatement(new StrSubstitutor(replaceMap).replace(expected), new ParsingOptions(AS_DECIMAL));

        String actualSql = SqlFormatter.formatSql(rewrittenStatement);

        assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedResult));
    }

    @DataProvider
    public Object[][] filter()
    {
        return new Object[][] {
                {"select p.name, filter(p.books, (book) -> book.name = 'book1' or book.name = 'book2') as filter_books from People p",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                " ${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                ", ${filter_cte} (userId, f1) AS (\n" +
                                "   SELECT\n" +
                                "     s.userId userId\n" +
                                "   , array_agg(t.bookId ORDER BY t.bookId ASC) filter(WHERE t.bookId IS NOT NULL) f1\n" +
                                "   FROM\n" +
                                "     ((${People.books} s\n" +
                                "   CROSS JOIN UNNEST(s.books) u (uc))\n" +
                                "   LEFT JOIN Book t ON (u.uc = t.bookId))\n" +
                                "   WHERE ((t.name = 'book1') OR (t.name = 'book2'))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  p.name\n" +
                                ", ${filter_cte}.f1 filter_books\n" +
                                "FROM\n" +
                                "  (People p\n" +
                                "LEFT JOIN ${filter_cte}\n" +
                                "ON (p.userId = ${filter_cte}.userId))"},
        };
    }

    @Test(dataProvider = "filter")
    public void testFilter(String original, String expected)
    {
        Statement statement = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        RelationshipCteGenerator generator = new RelationshipCteGenerator(oneToManyGraphMDL);
        Analysis analysis = StatementAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, oneToManyGraphMDL, generator);

        Statement rewrittenStatement = statement;
        for (GraphMDLRule rule : List.of(GRAPHMDL_SQL_REWRITE)) {
            rewrittenStatement = rule.apply(rewrittenStatement, DEFAULT_SESSION_CONTEXT, analysis, oneToManyGraphMDL);
        }

        Map<String, String> replaceMap = new HashMap<>();
        replaceMap.put("People.books", generator.getNameMapping().get("People.books"));
        replaceMap.put("filter_cte",
                generator.getNameMapping().get("filter(p.books, (book) -> ((book.name = 'book1') OR (book.name = 'book2')))"));

        Statement expectedResult = SQL_PARSER.createStatement(new StrSubstitutor(replaceMap).replace(expected), new ParsingOptions(AS_DECIMAL));
        String actualSql = SqlFormatter.formatSql(rewrittenStatement);
        assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedResult));
    }

    @DataProvider
    public Object[][] functionChain()
    {
        return new Object[][] {
                {"select p.name, transform(filter(p.books, (book) -> book.name = 'book1' or book.name = 'book2'), book -> book.name) as book_names from People p",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                " ${People.books} (userId, books) AS (\n" +
                                "   SELECT\n" +
                                "     o.userId userId\n" +
                                "   , array_agg(m.bookId ORDER BY m.bookId ASC) filter(WHERE m.bookId IS NOT NULL) books\n" +
                                "   FROM\n" +
                                "     (People o\n" +
                                "   LEFT JOIN Book m ON (o.userId = m.authorId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                ", ${filter_cte} (userId, f1) AS (\n" +
                                "   SELECT\n" +
                                "     s.userId userId\n" +
                                "   , array_agg(t.bookId ORDER BY t.bookId ASC) filter(WHERE t.bookId IS NOT NULL) f1\n" +
                                "   FROM\n" +
                                "     ((${People.books} s\n" +
                                "   CROSS JOIN UNNEST(s.books) u (uc))\n" +
                                "   LEFT JOIN Book t ON (u.uc = t.bookId))\n" +
                                "   WHERE ((t.name = 'book1') OR (t.name = 'book2'))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                ", ${transform_cte} (userId, f1) AS (\n" +
                                "   SELECT\n" +
                                "     s.userId userId\n" +
                                "   , array_agg(t.name ORDER BY t.bookId ASC) filter(WHERE t.name IS NOT NULL) f1\n" +
                                "   FROM\n" +
                                "     ((${filter_cte} s\n" +
                                "   CROSS JOIN UNNEST(s.f1) u (uc))\n" +
                                "   LEFT JOIN Book t ON (u.uc = t.bookId))\n" +
                                "   GROUP BY 1\n" +
                                ") \n" +
                                "SELECT\n" +
                                "  p.name\n" +
                                ", ${transform_cte}.f1 book_names\n" +
                                "FROM\n" +
                                "  (People p\n" +
                                "LEFT JOIN ${transform_cte}\n" +
                                "ON (p.userId = ${transform_cte}.userId))"},
        };
    }

    @Test(dataProvider = "functionChain")
    public void testFunctionChain(String original, String expected)
    {
        Statement statement = SQL_PARSER.createStatement(original, new ParsingOptions(AS_DECIMAL));
        RelationshipCteGenerator generator = new RelationshipCteGenerator(oneToManyGraphMDL);
        Analysis analysis = StatementAnalyzer.analyze(statement, DEFAULT_SESSION_CONTEXT, oneToManyGraphMDL, generator);

        Statement rewrittenStatement = statement;
        for (GraphMDLRule rule : List.of(GRAPHMDL_SQL_REWRITE)) {
            rewrittenStatement = rule.apply(rewrittenStatement, DEFAULT_SESSION_CONTEXT, analysis, oneToManyGraphMDL);
        }

        Map<String, String> replaceMap = new HashMap<>();
        replaceMap.put("People.books", generator.getNameMapping().get("People.books"));
        replaceMap.put("filter_cte",
                generator.getNameMapping().get("filter(p.books, (book) -> ((book.name = 'book1') OR (book.name = 'book2')))"));
        replaceMap.put("transform_cte",
                generator.getNameMapping().get("transform(filter(p.books, (book) -> ((book.name = 'book1') OR (book.name = 'book2'))), (book) -> book.name)"));

        Statement expectedResult = SQL_PARSER.createStatement(new StrSubstitutor(replaceMap).replace(expected), new ParsingOptions(AS_DECIMAL));
        String actualSql = SqlFormatter.formatSql(rewrittenStatement);
        assertThat(actualSql).isEqualTo(SqlFormatter.formatSql(expectedResult));
    }
}
