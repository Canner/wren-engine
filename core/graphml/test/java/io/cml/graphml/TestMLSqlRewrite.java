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

import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.cml.graphml.MLSqlRewrite.rewrite;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMLSqlRewrite
{
    @Test
    public void testModelRewrite()
            throws URISyntaxException, IOException
    {
        GraphML graphML = GraphML.fromJson(Files.readString(Path.of(
                requireNonNull(getClass().getClassLoader().getResource("TestMLSqlRewrite.json")).toURI())));

        // no rewrite since foo is not a model
        assertSqlEquals(rewrite("SELECT * FROM foo", graphML), "SELECT * FROM foo");

        assertSqlEquals(rewrite("SELECT * FROM User", graphML),
                "WITH User AS (SELECT id, email FROM (SELECT * FROM User)) SELECT * FROM User");
        assertSqlEquals(rewrite("SELECT * FROM Book", graphML),
                "WITH Book AS (SELECT authorId, publish_date FROM (SELECT * FROM Book)) SELECT * FROM Book");
        assertSqlEquals(rewrite("SELECT * FROM User WHERE id = 'SN1001'", graphML),
                "WITH User AS (SELECT id, email FROM (SELECT * FROM User)) SELECT * FROM User WHERE id = 'SN1001'");

        assertSqlEquals(rewrite("SELECT * FROM User a join Book b ON a.id = b.authorId WHERE a.id = 'SN1001'", graphML),
                "WITH Book AS (SELECT authorId, publish_date FROM (SELECT * FROM Book)),\n" +
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
