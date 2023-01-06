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

package io.cml.graphml;

import io.cml.graphml.base.GraphML;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.testng.annotations.Test;

import java.util.List;

import static io.cml.graphml.ModelSqlRewrite.MODEL_SQL_REWRITE;
import static io.cml.graphml.base.dto.Column.column;
import static io.cml.graphml.base.dto.Manifest.manifest;
import static io.cml.graphml.base.dto.Model.model;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestModelSqlRewrite
{
    @Test
    public void testModelRewrite()
    {
        GraphML graphML = GraphML.fromManifest(manifest(
                List.of(model(
                                "User",
                                "SELECT * FROM User",
                                List.of(
                                        column("id", "STRING", null, false),
                                        column("email", "STRING", null, false))),
                        model(
                                "Book",
                                "SELECT * FROM Book",
                                List.of(
                                        column("authorId", "STRING", null, false),
                                        column("publish_date", "STRING", null, false),
                                        column("publish_year", "DATE", null, false, "date_trunc('year', publish_date)")))),
                List.of(),
                List.of(),
                List.of()));

        // no rewrite since foo is not a model
        assertSqlEquals(rewrite("SELECT * FROM foo", graphML), "SELECT * FROM foo");

        assertSqlEquals(rewrite("SELECT * FROM User", graphML),
                "WITH User AS (SELECT id, email FROM (SELECT * FROM User)) SELECT * FROM User");
        assertSqlEquals(rewrite("SELECT * FROM Book", graphML),
                "WITH Book AS (SELECT authorId, publish_date, date_trunc('year', publish_date) publish_year FROM (SELECT * FROM Book)) SELECT * FROM Book");
        assertSqlEquals(rewrite("SELECT * FROM User WHERE id = 'SN1001'", graphML),
                "WITH User AS (SELECT id, email FROM (SELECT * FROM User)) SELECT * FROM User WHERE id = 'SN1001'");

        assertSqlEquals(rewrite("SELECT * FROM User a join Book b ON a.id = b.authorId WHERE a.id = 'SN1001'", graphML),
                "WITH Book AS (SELECT authorId, publish_date, date_trunc('year', publish_date) publish_year FROM (SELECT * FROM Book)),\n" +
                        "User AS (SELECT id, email FROM (SELECT * FROM User))\n" +
                        "SELECT * FROM User a join Book b ON a.id = b.authorId WHERE a.id = 'SN1001'");

        assertSqlEquals(rewrite("SELECT * FROM User a join foo b ON a.id = b.id WHERE a.id = 'SN1001'", graphML),
                "WITH User AS (SELECT id, email FROM (SELECT * FROM User))\n" +
                        "SELECT * FROM User a join foo b ON a.id = b.id WHERE a.id = 'SN1001'");

        assertSqlEquals(rewrite("WITH a AS (SELECT * FROM foo) SELECT * FROM a JOIN User ON a.id = User.id", graphML),
                "WITH User AS (SELECT id, email FROM (SELECT * FROM User)), a AS (SELECT * FROM foo)\n" +
                        "SELECT * FROM a JOIN User ON a.id = User.id");

        // rewrite table in with query
        assertSqlEquals(rewrite("WITH a AS (SELECT * FROM User) SELECT * FROM a", graphML),
                "WITH User AS (SELECT id, email FROM (SELECT * FROM User)),\n" +
                        "a AS (SELECT * FROM User)\n" +
                        "SELECT * FROM a");
    }

    private String rewrite(String sql, GraphML graphML)
    {
        return GraphMLPlanner.rewrite(sql, graphML, List.of(MODEL_SQL_REWRITE));
    }

    private static void assertSqlEquals(String actual, String expected)
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
