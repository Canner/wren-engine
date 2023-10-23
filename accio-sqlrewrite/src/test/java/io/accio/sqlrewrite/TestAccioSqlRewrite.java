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

import io.accio.base.AccioMDL;
import io.accio.testing.AbstractTestFramework;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Model.model;
import static io.accio.sqlrewrite.AccioSqlRewrite.ACCIO_SQL_REWRITE;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestAccioSqlRewrite
        extends AbstractTestFramework
{
    private static final AccioMDL ACCIOMDL = AccioMDL.fromManifest(withDefaultCatalogSchema()
            .setModels(List.of(
                    model(
                            "People",
                            "SELECT * FROM People",
                            List.of(
                                    column("id", "STRING", null, false),
                                    column("email", "STRING", null, false))),
                    model(
                            "Book",
                            "SELECT * FROM Book",
                            List.of(
                                    column("authorId", "STRING", null, false),
                                    column("publish_date", "STRING", null, false),
                                    column("publish_year", "DATE", null, false, "date_trunc('year', publish_date)")))))
            .build());

    @Override
    protected void prepareData()
    {
        exec("CREATE TABLE People AS SELECT * FROM\n" +
                "(VALUES\n" +
                "('SN1001', 'foo@foo.org'),\n" +
                "('SN1002', 'bar@bar.org'),\n" +
                "('SN1003', 'code@code.org'))\n" +
                "People (id, email)");
        exec("CREATE TABLE Book AS SELECT * FROM\n" +
                "(VALUES\n" +
                "('P1001', CAST('1991-01-01' AS TIMESTAMP)),\n" +
                "('P1002', CAST('1992-02-02' AS TIMESTAMP)),\n" +
                "('P1003', CAST('1993-03-03' AS TIMESTAMP)))\n" +
                "Book (authorId, publish_date)");
        exec("CREATE TABLE WishList AS SELECT * FROM\n" +
                "(VALUES\n" +
                "('SN1001'),\n" +
                "('SN1002'),\n" +
                "('SN10010'))\n" +
                "WishList (id)");
    }

    @Test
    public void testModelRewrite()
    {
        assertSqlEqualsAndValid(rewrite("SELECT * FROM People"),
                "WITH People AS (SELECT id, email FROM (SELECT * FROM People) t) SELECT * FROM People");
        assertSqlEqualsAndValid(rewrite("SELECT * FROM Book"),
                "WITH Book AS (SELECT authorId, publish_date, date_trunc('year', publish_date) publish_year FROM (SELECT * FROM Book) t) SELECT * FROM Book");
        assertSqlEqualsAndValid(rewrite("SELECT * FROM People WHERE id = 'SN1001'"),
                "WITH People AS (SELECT id, email FROM (SELECT * FROM People) t) SELECT * FROM People WHERE id = 'SN1001'");

        assertSqlEqualsAndValid(rewrite("SELECT * FROM People a join Book b ON a.id = b.authorId WHERE a.id = 'SN1001'"),
                "WITH Book AS (SELECT authorId, publish_date, date_trunc('year', publish_date) publish_year FROM (SELECT * FROM Book) t),\n" +
                        "People AS (SELECT id, email FROM (SELECT * FROM People) t)\n" +
                        "SELECT * FROM People a join Book b ON a.id = b.authorId WHERE a.id = 'SN1001'");

        assertSqlEqualsAndValid(rewrite("SELECT * FROM People a join WishList b ON a.id = b.id WHERE a.id = 'SN1001'"),
                "WITH People AS (SELECT id, email FROM (SELECT * FROM People) t)\n" +
                        "SELECT * FROM People a join WishList b ON a.id = b.id WHERE a.id = 'SN1001'");

        assertSqlEqualsAndValid(rewrite("WITH a AS (SELECT * FROM WishList) SELECT * FROM a JOIN People ON a.id = People.id"),
                "WITH People AS (SELECT id, email FROM (SELECT * FROM People) t), a AS (SELECT * FROM WishList)\n" +
                        "SELECT * FROM a JOIN People ON a.id = People.id");

        // rewrite table in with query
        assertSqlEqualsAndValid(rewrite("WITH a AS (SELECT * FROM People) SELECT * FROM a"),
                "WITH People AS (SELECT id, email FROM (SELECT * FROM People) t),\n" +
                        "a AS (SELECT * FROM People)\n" +
                        "SELECT * FROM a");
    }

    @Test
    public void testNoRewrite()
    {
        assertSqlEquals(rewrite("SELECT * FROM WithList"), "SELECT * FROM WithList");
    }

    private String rewrite(String sql)
    {
        return AccioPlanner.rewrite(sql, DEFAULT_SESSION_CONTEXT, ACCIOMDL, List.of(ACCIO_SQL_REWRITE));
    }

    private void assertSqlEqualsAndValid(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertSqlEquals(actual, expected);
        assertThatNoException()
                .describedAs(format("actual sql: %s is invalid", actual))
                .isThrownBy(() -> query(actual));
    }

    private void assertSqlEquals(String actual, String expected)
    {
        SqlParser sqlParser = new SqlParser();
        ParsingOptions parsingOptions = new ParsingOptions(AS_DECIMAL);
        Statement actualStmt = sqlParser.createStatement(actual, parsingOptions);
        Statement expectedStmt = sqlParser.createStatement(expected, parsingOptions);
        assertThat(actualStmt)
                .describedAs("%n[actual]%n%s[expect]%n%s",
                        SqlFormatter.formatSql(actualStmt), SqlFormatter.formatSql(expectedStmt))
                .isEqualTo(expectedStmt);
    }
}
