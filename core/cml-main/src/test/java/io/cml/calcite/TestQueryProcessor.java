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

package io.cml.calcite;

import io.trino.sql.tree.QualifiedName;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Set;

import static io.cml.calcite.QueryProcessor.extractTables;
import static org.apache.calcite.avatica.util.Casing.UNCHANGED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryProcessor
{
    @DataProvider
    public Object[][] forTestExtractTables()
    {
        return new Object[][] {
                {"SELECT * FROM schema.t1", Set.of(QualifiedName.of("schema", "t1"))},
                {"SELECT * FROM db.schema.t1", Set.of(QualifiedName.of("db", "schema", "t1"))},
                {"SELECT * FROM db.schema.T1", Set.of(QualifiedName.of("db", "schema", "T1"))},
                {"SELECT * FROM \"db\".\"schema\".\"T1\"", Set.of(QualifiedName.of("db", "schema", "T1"))},
                {"SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.b", Set.of(QualifiedName.of("t1"), QualifiedName.of("t2"))},
                {"SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.b LEFT JOIN t3 ON t1.a = t3.c",
                        Set.of(QualifiedName.of("t1"), QualifiedName.of("t2"), QualifiedName.of("t3"))},
                {"SELECT * FROM (SELECT * FROM t1 WHERE a = 'foo')", Set.of(QualifiedName.of("t1"))},
                {"SELECT * FROM t1 AS t", Set.of(QualifiedName.of("t1"))},
                {"SELECT * FROM (VALUES('1', '2')) AS t(a, b)", Set.of()},
                {"SELECT a FROM t1 UNION ALL SELECT a FROM t2 ORDER BY a", Set.of(QualifiedName.of("t1"), QualifiedName.of("t2"))},
                {"SELECT * FROM t1, t2 WHERE t1.id = t2.id", Set.of(QualifiedName.of("t1"), QualifiedName.of("t2"))}
        };
    }

    @Test(dataProvider = "forTestExtractTables")
    public void testExtractTables(String sql, Set<QualifiedName> expected)
            throws SqlParseException
    {
        // Calcite default use UPPERCASE in withUnquotedCasing
        SqlParser.Config config = SqlParser.config().withUnquotedCasing(UNCHANGED);
        assertThat(extractTables(SqlParser.create(sql, config).parseQuery(), false))
                .isEqualTo(expected);
    }
}
