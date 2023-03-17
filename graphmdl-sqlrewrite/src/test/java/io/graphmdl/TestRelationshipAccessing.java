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
import io.graphmdl.base.dto.Relationship;
import io.graphmdl.sqlrewrite.GraphMDLPlanner;
import io.graphmdl.sqlrewrite.GraphMDLRule;
import io.graphmdl.sqlrewrite.RelationshipCteGenerator;
import io.graphmdl.sqlrewrite.Utils;
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
                // TODO: fix relationship columns with table alias prefix (e.g. a.author.book.author.name)
//                {"SELECT a.author.book.author.name\n" +
//                        "FROM Book a",
//                        EXPECTED_AUTHOR_BOOK_AUTHOR_WITH_QUERIES +
//                                "SELECT ${Book.author.book.author}.name\n" +
//                                "FROM\n" +
//                                "  (Book\n" +
//                                "LEFT JOIN ${Book.author.book.author} ON (Book.authorId = ${Book.author.book.author}.userId))",
//                        true},
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
            rewrittenStatement = rule.apply(rewrittenStatement, DEFAULT_SESSION_CONTEXT, analysis, oneToOneGraphMDL);
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
    public Object[][] oneToManyRelationshipAccessCase()
    {
        return new Object[][] {
                {"SELECT books[1].name FROM People",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.books} (userId, name, sorted_books, books) AS (\n" +
                                "   SELECT\n" +
                                "     one.userId userId\n" +
                                "   , one.name name\n" +
                                "   , one.sorted_books sorted_books\n" +
                                "   , array_agg(many.bookId ORDER BY many.bookId ASC) books\n" +
                                "   FROM\n" +
                                "     (People one\n" +
                                "   LEFT JOIN Book many ON (one.userId = many.authorId))\n" +
                                "   GROUP BY 1, 2, 3\n" +
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
                                "${People.books} (userId, name, sorted_books, books) AS (\n" +
                                "   SELECT\n" +
                                "     one.userId userId\n" +
                                "   , one.name name\n" +
                                "   , one.sorted_books sorted_books\n" +
                                "   , array_agg(many.bookId ORDER BY many.bookId ASC) books\n" +
                                "   FROM\n" +
                                "     (People one\n" +
                                "   LEFT JOIN Book many ON (one.userId = many.authorId))\n" +
                                "   GROUP BY 1, 2, 3\n" +
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
                                ", ${People.books[1].author.books} (userId, name, sorted_books, books) AS (\n" +
                                "   SELECT\n" +
                                "     one.userId userId\n" +
                                "   , one.name name\n" +
                                "   , one.sorted_books sorted_books\n" +
                                "   , array_agg(many.bookId ORDER BY many.bookId ASC) books\n" +
                                "   FROM\n" +
                                "     (${People.books[1].author} one\n" +
                                "   LEFT JOIN Book many ON (one.userId = many.authorId))\n" +
                                "   GROUP BY 1, 2, 3\n" +
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
                                "${People.books} (userId, name, sorted_books, books) AS (\n" +
                                "   SELECT\n" +
                                "     one.userId userId\n" +
                                "   , one.name name\n" +
                                "   , one.sorted_books sorted_books\n" +
                                "   , array_agg(many.bookId ORDER BY many.bookId ASC) books\n" +
                                "   FROM\n" +
                                "     (People one\n" +
                                "   LEFT JOIN Book many ON (one.userId = many.authorId))\n" +
                                "   GROUP BY 1, 2, 3\n" +
                                ") \n" +
                                "SELECT cardinality(${People.books}.books)\n" +
                                "FROM\n" +
                                "  (People\n" +
                                "LEFT JOIN ${People.books} ON (People.userId = ${People.books}.userId))", false},
                {"SELECT cardinality(People.books) FROM People",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.books} (userId, name, sorted_books, books) AS (\n" +
                                "   SELECT\n" +
                                "     one.userId userId\n" +
                                "   , one.name name\n" +
                                "   , one.sorted_books sorted_books\n" +
                                "   , array_agg(many.bookId ORDER BY many.bookId ASC) books\n" +
                                "   FROM\n" +
                                "     (People one\n" +
                                "   LEFT JOIN Book many ON (one.userId = many.authorId))\n" +
                                "   GROUP BY 1, 2, 3\n" +
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
                                ", ${Book.author.books} (userId, name, sorted_books, books) AS (\n" +
                                "   SELECT\n" +
                                "     one.userId userId\n" +
                                "   , one.name name\n" +
                                "   , one.sorted_books sorted_books\n" +
                                "   , array_agg(many.bookId ORDER BY many.bookId ASC) books\n" +
                                "   FROM\n" +
                                "     (${Book.author} one\n" +
                                "   LEFT JOIN Book many ON (one.userId = many.authorId))\n" +
                                "   GROUP BY 1, 2, 3\n" +
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
                                "${People.sorted_books} (userId, name, books, sorted_books) AS (\n" +
                                "   SELECT\n" +
                                "     one.userId userId\n" +
                                "   , one.name name\n" +
                                "   , one.books books\n" +
                                "   , array_agg(many.bookId ORDER BY many.name ASC, many.bookId DESC) sorted_books\n" +
                                "   FROM\n" +
                                "     (People one\n" +
                                "   LEFT JOIN Book many ON (one.userId = many.authorId))\n" +
                                "   GROUP BY 1, 2, 3\n" +
                                ") \n" +
                                "SELECT cardinality(${People.sorted_books}.sorted_books)\n" +
                                "FROM\n" +
                                "  (People\n" +
                                "LEFT JOIN ${People.sorted_books} ON (People.userId = ${People.sorted_books}.userId))", false},
                {"SELECT sorted_books[1].name FROM People",
                        "WITH\n" + ONE_TO_MANY_MODEL_CTE + ",\n" +
                                "${People.sorted_books} (userId, name, books, sorted_books) AS (\n" +
                                "   SELECT\n" +
                                "     one.userId userId\n" +
                                "   , one.name name\n" +
                                "   , one.books books\n" +
                                "   , array_agg(many.bookId ORDER BY many.name ASC, many.bookId DESC) sorted_books\n" +
                                "   FROM\n" +
                                "     (People one\n" +
                                "   LEFT JOIN Book many ON (one.userId = many.authorId))\n" +
                                "   GROUP BY 1, 2, 3\n" +
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
                // TODO: support relation with alias
                // {"SELECT array_length(books) FROM People p", "Select 1", false},
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
            rewrittenStatement = rule.apply(rewrittenStatement, DEFAULT_SESSION_CONTEXT, analysis, oneToManyGraphMDL);
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
}
