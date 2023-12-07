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
import io.accio.testing.AbstractTestFramework;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.dto.Column.caluclatedColumn;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Column.relationshipColumn;
import static io.accio.base.dto.JoinType.ONE_TO_MANY;
import static io.accio.base.dto.JoinType.ONE_TO_ONE;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.Model.onBaseObject;
import static io.accio.base.dto.Relationship.relationship;
import static io.accio.sqlrewrite.AccioSqlRewrite.ACCIO_SQL_REWRITE;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestModelSqlRewrite
        extends AbstractTestFramework
{
    private static final Manifest DEFAULT_MANIFEST = withDefaultCatalogSchema()
            .setRelationships(List.of(
                    relationship("WishListPeople", List.of("WishList", "People"), ONE_TO_ONE, "WishList.id = People.id"),
                    relationship("PeopleBook", List.of("People", "Book"), ONE_TO_MANY, "People.id = Book.authorId")))
            .setModels(List.of(
                    model(
                            "People",
                            "SELECT * FROM table_people",
                            List.of(
                                    column("id", "STRING", null, false),
                                    column("email", "STRING", null, false),
                                    caluclatedColumn("gift", "STRING", "wishlist.bookId"),
                                    relationshipColumn("book", "Book", "PeopleBook"),
                                    relationshipColumn("wishlist", "WishList", "WishListPeople")),
                            "id"),
                    model(
                            "Book",
                            "SELECT * FROM table_book",
                            List.of(
                                    column("bookId", "STRING", null, false),
                                    column("authorId", "STRING", null, false),
                                    column("publish_date", "STRING", null, false),
                                    column("publish_year", "DATE", null, false, "date_trunc('year', publish_date)"),
                                    caluclatedColumn("author_gift_id", "STRING", "people.wishlist.bookId"),
                                    relationshipColumn("people", "People", "PeopleBook")),
                            "bookId"),
                    model(
                            "WishList",
                            "SELECT * FROM table_wishlist",
                            List.of(
                                    column("id", "STRING", null, false),
                                    column("bookId", "STRING", null, false)),
                            "id")))
            .build();
    private static final AccioMDL ACCIOMDL = AccioMDL.fromManifest(DEFAULT_MANIFEST);

    /**
     * TODO: Refactor the test. Currently we assert the SQL string is fully equal to the expected.
     *  It's hard to maintain and meaningless. We should assert the generated SQL contains the specific
     *  pattern and the result is correct.
     */
    @Language("SQL")
    private static final String WITH_PEOPLE_QUERY = "" +
            "  WishList AS (\n" +
            "   SELECT\n" +
            "     \"WishList\".\"id\" \"id\"\n" +
            "   , \"WishList\".\"bookId\" \"bookId\"\n" +
            "   FROM\n" +
            "     (\n" +
            "      SELECT\n" +
            "        \"WishList\".\"id\" \"id\"\n" +
            "      , \"WishList\".\"bookId\" \"bookId\"\n" +
            "      FROM\n" +
            "        (\n" +
            "         SELECT *\n" +
            "         FROM\n" +
            "           table_wishlist\n" +
            "      )  \"WishList\"\n" +
            "   )  \"WishList\"\n" +
            ") \n" +
            ", People AS (\n" +
            "   SELECT\n" +
            "     \"People\".\"id\" \"id\"\n" +
            "   , \"People\".\"email\" \"email\"\n" +
            "   , \"People_relationsub\".\"gift\" \"gift\"\n" +
            "   FROM\n" +
            "     ((\n" +
            "      SELECT\n" +
            "        \"People\".\"id\" \"id\"\n" +
            "      , \"People\".\"email\" \"email\"\n" +
            "      FROM\n" +
            "        (\n" +
            "         SELECT *\n" +
            "         FROM\n" +
            "           table_people\n" +
            "      )  \"People\"\n" +
            "   )  \"People\"\n" +
            "   LEFT JOIN (\n" +
            "      SELECT\n" +
            "        \"People\".\"id\"\n" +
            "      , \"WishList\".\"bookId\" \"gift\"\n" +
            "      FROM\n" +
            "        ((\n" +
            "         SELECT\n" +
            "           \"id\"\n" +
            "         , \"email\"\n" +
            "         FROM\n" +
            "           (\n" +
            "            SELECT *\n" +
            "            FROM\n" +
            "              table_people\n" +
            "         )  \"People\"\n" +
            "      )  \"People\"\n" +
            "      LEFT JOIN \"WishList\" ON (WishList.id = People.id))\n" +
            "   )  \"People_relationsub\" ON (\"People\".\"id\" = \"People_relationsub\".\"id\"))\n" +
            ")\n";

    @Language("SQL")
    private static final String WITH_BOOK_QUERY = WITH_PEOPLE_QUERY +
            ", Book AS (\n" +
            "   SELECT\n" +
            "     \"Book\".\"bookId\" \"bookId\"\n" +
            "   , \"Book\".\"authorId\" \"authorId\"\n" +
            "   , \"Book\".\"publish_date\" \"publish_date\"\n" +
            "   , \"Book\".\"publish_year\" \"publish_year\"\n" +
            "   , \"Book_relationsub\".\"author_gift_id\" \"author_gift_id\"\n" +
            "   FROM\n" +
            "     ((\n" +
            "      SELECT\n" +
            "        \"Book\".\"bookId\" \"bookId\"\n" +
            "      , \"Book\".\"authorId\" \"authorId\"\n" +
            "      , \"Book\".\"publish_date\" \"publish_date\"\n" +
            "      , date_trunc('year', publish_date) \"publish_year\"\n" +
            "      FROM\n" +
            "        (\n" +
            "         SELECT *\n" +
            "         FROM\n" +
            "           table_book\n" +
            "      )  \"Book\"\n" +
            "   )  \"Book\"\n" +
            "   LEFT JOIN (\n" +
            "      SELECT\n" +
            "        \"Book\".\"bookId\"\n" +
            "      , \"WishList\".\"bookId\" \"author_gift_id\"\n" +
            "      FROM\n" +
            "        (((\n" +
            "         SELECT\n" +
            "           \"bookId\"\n" +
            "         , \"authorId\"\n" +
            "         , \"publish_date\"\n" +
            "         , date_trunc('year', publish_date) \"publish_year\"\n" +
            "         FROM\n" +
            "           (\n" +
            "            SELECT *\n" +
            "            FROM\n" +
            "              table_book\n" +
            "         )  \"Book\"\n" +
            "      )  \"Book\"\n" +
            "      LEFT JOIN \"People\" ON (People.id = Book.authorId))\n" +
            "      LEFT JOIN \"WishList\" ON (WishList.id = People.id))\n" +
            "   )  \"Book_relationsub\" ON (\"Book\".\"bookId\" = \"Book_relationsub\".\"bookId\"))\n" +
            ")\n";

    @Override
    protected void prepareData()
    {
        exec("CREATE TABLE table_people AS SELECT * FROM\n" +
                "(VALUES\n" +
                "('P1001', 'foo@foo.org'),\n" +
                "('P1002', 'bar@bar.org'))\n" +
                "People (id, email)");
        exec("CREATE TABLE table_book AS SELECT * FROM\n" +
                "(VALUES\n" +
                "('SN1001', 'P1001', CAST('1991-01-01' AS TIMESTAMP)),\n" +
                "('SN1002', 'P1002', CAST('1992-02-02' AS TIMESTAMP)),\n" +
                "('SN1003', 'P1001', CAST('1993-03-03' AS TIMESTAMP)))\n" +
                "Book (bookId, authorId, publish_date)");
        exec("CREATE TABLE table_wishlist AS SELECT * FROM\n" +
                "(VALUES\n" +
                "('P1001', 'SN1002'),\n" +
                "('P1002', 'SN1001'))\n" +
                "WishList (id, bookId)");
    }

    @Override
    protected void cleanup()
    {
        exec("DROP TABLE table_people");
        exec("DROP TABLE table_book");
        exec("DROP TABLE table_wishlist");
    }

    @Test
    public void testModelRewrite()
    {
        assertSqlEqualsAndValid(rewrite("SELECT * FROM People"), "WITH " + WITH_PEOPLE_QUERY + "SELECT * FROM People");
        assertSqlEqualsAndValid(rewrite("SELECT * FROM People WHERE id = 'SN1001'"), "WITH " + WITH_PEOPLE_QUERY + "SELECT * FROM People WHERE id = 'SN1001'");
        assertSqlEqualsAndValid(rewrite("SELECT * FROM Book"), "WITH " + WITH_BOOK_QUERY + "SELECT * FROM Book");
        assertSqlEqualsAndValid(rewrite("SELECT * FROM People a join Book b ON a.id = b.authorId WHERE a.id = 'SN1001'"),
                "WITH " + WITH_BOOK_QUERY + "SELECT * FROM People a join Book b ON a.id = b.authorId WHERE a.id = 'SN1001'");
        assertSqlEqualsAndValid(rewrite("SELECT * FROM People a join WishList b ON a.id = b.id WHERE a.id = 'SN1001'"),
                "WITH " + WITH_PEOPLE_QUERY + "SELECT * FROM People a join WishList b ON a.id = b.id WHERE a.id = 'SN1001'");

        assertSqlEqualsAndValid(rewrite("WITH a AS (SELECT * FROM WishList) SELECT * FROM a JOIN People ON a.id = People.id"),
                "WITH" + WITH_PEOPLE_QUERY + ", a AS (SELECT * FROM WishList) SELECT * FROM a JOIN People ON a.id = People.id");
        // rewrite table in with query
        assertSqlEqualsAndValid(rewrite("WITH a AS (SELECT * FROM People) SELECT * FROM a"),
                "WITH" + WITH_PEOPLE_QUERY + ", a AS (SELECT * FROM People) SELECT * FROM a");
    }

    @Test
    public void testCycle()
    {
        AccioMDL cycle = AccioMDL.fromManifest(withDefaultCatalogSchema()
                .setRelationships(List.of(
                        relationship("WishListPeople", List.of("WishList", "People"), ONE_TO_ONE, "WishList.id = People.id")))
                .setModels(List.of(
                        model(
                                "People",
                                "SELECT * FROM People",
                                List.of(
                                        column("id", "STRING", null, false),
                                        column("email", "STRING", null, false),
                                        caluclatedColumn("gift", "STRING", "wishlist.bookId"),
                                        relationshipColumn("wishlist", "WishList", "WishListPeople")),
                                "id"),
                        model(
                                "WishList",
                                "SELECT * FROM WishList",
                                List.of(
                                        column("id", "STRING", null, false),
                                        column("bookId", "STRING", null, false),
                                        caluclatedColumn("peopleId", "STRING", "people.id"),
                                        relationshipColumn("people", "People", "WishListPeople")),
                                "id")))
                .build());

        // TODO: This is not allowed since accio lack of the functionality of analyzing select items in model in sql.
        //  Currently we treat all columns in models are required, and that cause cycles in generating WITH queries when models reference each other.
        assertThatThrownBy(() -> rewrite("SELECT * FROM People", cycle))
                .hasMessage("found cycle in models");
    }

    @Test
    public void testNoRewrite()
    {
        assertSqlEquals(rewrite("SELECT * FROM foo"), "SELECT * FROM foo");
    }

    @Test
    public void testModelOnModel()
    {
        List<Model> models = ImmutableList.<Model>builder()
                .addAll(DEFAULT_MANIFEST.getModels())
                .add(
                        onBaseObject(
                                "BookReplica",
                                "Book",
                                List.of(
                                        column("id", "STRING", null, false, "bookId"),
                                        column("authorId", "STRING", null, false),
                                        column("publish_year", "DATE", null, false, "date_trunc('year', publish_date)"),
                                        column("author_gift_id", "STRING", null, false),
                                        caluclatedColumn("wishlist_id", "STRING", "wishlist.id"),
                                        relationshipColumn("wishlist", "WishList", "BookReplicaWishList")),
                                "id"))
                .build();
        List<Relationship> relationships = ImmutableList.<Relationship>builder()
                .addAll(DEFAULT_MANIFEST.getRelationships())
                .add(relationship("BookReplicaWishList", List.of("BookReplica", "WishList"), ONE_TO_ONE, "BookReplica.id = WishList.bookId"))
                .build();
        AccioMDL mdl = AccioMDL.fromManifest(
                copyOf(DEFAULT_MANIFEST)
                        .setModels(models)
                        .setRelationships(relationships)
                        .build());

        @Language("SQL")
        String bookReplica = "" +
                ", BookReplica AS (\n" +
                "   SELECT\n" +
                "     \"BookReplica\".\"authorId\" \"authorId\"\n" +
                "   , \"BookReplica\".\"author_gift_id\" \"author_gift_id\"\n" +
                "   , \"BookReplica\".\"id\" \"id\"\n" +
                "   , \"BookReplica\".\"publish_year\" \"publish_year\"\n" +
                "   , \"BookReplica_relationsub\".\"wishlist_id\" \"wishlist_id\"\n" +
                "   FROM\n" +
                "     ((\n" +
                "      SELECT\n" +
                "        \"BookReplica\".\"authorId\" \"authorId\"\n" +
                "      , \"BookReplica\".\"author_gift_id\" \"author_gift_id\"\n" +
                "      , bookId \"id\"\n" +
                "      , date_trunc('year', publish_date) \"publish_year\"\n" +
                "      FROM\n" +
                "        (\n" +
                "         SELECT *\n" +
                "         FROM\n" +
                "           Book\n" +
                "      )  \"BookReplica\"\n" +
                "   )  \"BookReplica\"\n" +
                "   LEFT JOIN (\n" +
                "      SELECT\n" +
                "        \"BookReplica\".\"id\"\n" +
                "      , \"WishList\".\"id\" \"wishlist_id\"\n" +
                "      FROM\n" +
                "        ((\n" +
                "         SELECT\n" +
                "           bookId \"id\"\n" +
                "         , \"authorId\"\n" +
                "         , date_trunc('year', publish_date) \"publish_year\"\n" +
                "         , \"author_gift_id\"\n" +
                "         FROM\n" +
                "           (\n" +
                "            SELECT *\n" +
                "            FROM\n" +
                "              Book\n" +
                "         )  \"BookReplica\"\n" +
                "      )  \"BookReplica\"\n" +
                "      LEFT JOIN \"WishList\" ON (BookReplica.id = WishList.bookId))\n" +
                "   )  \"BookReplica_relationsub\" ON (\"BookReplica\".\"id\" = \"BookReplica_relationsub\".\"id\"))\n" +
                ") ";

        assertSqlEqualsAndValid(rewrite("SELECT * FROM BookReplica", mdl),
                "WITH " + (WITH_BOOK_QUERY + bookReplica) + "SELECT * FROM BookReplica");
        assertSqlEqualsAndValid(rewrite("SELECT * FROM BookReplica br JOIN People p ON br.authorId = p.id", mdl),
                "WITH " + (WITH_BOOK_QUERY + bookReplica) + "SELECT * FROM BookReplica br JOIN People p ON br.authorId = p.id");
    }

    private static String rewrite(String sql)
    {
        return rewrite(sql, ACCIOMDL);
    }

    private static String rewrite(String sql, AccioMDL mdl)
    {
        return AccioPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, mdl, List.of(ACCIO_SQL_REWRITE));
    }

    private static void assertSqlEquals(String actual, String expected)
    {
        SqlParser sqlParser = new SqlParser();
        ParsingOptions parsingOptions = new ParsingOptions(AS_DECIMAL);
        Statement actualStmt = sqlParser.createStatement(actual, parsingOptions);
        Statement expectedStmt = sqlParser.createStatement(expected, parsingOptions);
        assertThat(formatSql(actualStmt))
                .isEqualTo(formatSql(expectedStmt));
    }

    private void assertSqlEqualsAndValid(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertSqlEquals(actual, expected);
        assertThatNoException()
                .describedAs(format("actual sql: %s is invalid", actual))
                .isThrownBy(() -> query(actual));
    }
}
