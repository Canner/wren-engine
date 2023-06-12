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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.graphmdl.sqlrewrite.AnyFunctionSqlRewrite.ANY_FUNCTION_SQL_REWRITE;
import static io.graphmdl.sqlrewrite.Utils.parseSql;
import static io.trino.sql.SqlFormatter.formatSql;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAnyFunctionSqlRewrite
{
    @DataProvider
    public Object[][] rewrite()
    {
        return new Object[][] {
                {"SELECT any(filter(rs, rs -> rs.col1 = 'F')) FROM model", "SELECT filter(rs, rs -> rs.col1 = 'F')[1] FROM model"},
                {"SELECT any(filter(rs, rs -> rs.col1 = 'F')) IS NOT NULL FROM model", "SELECT filter(rs, rs -> rs.col1 = 'F')[1] IS NOT NULL FROM model"},
                {"SELECT any(filter(rs, rs -> rs.col1 = 'F')).col1 + 1 FROM model", "SELECT filter(rs, rs -> rs.col1 = 'F')[1].col1 + 1 FROM model"},
                {"SELECT any(filter(rs, rs -> rs.col1 = 'F')) AS a FROM model", "SELECT filter(rs, rs -> rs.col1 = 'F')[1] AS a FROM model"},
                {"SELECT any(filter(rs, rs -> rs.col1 = 'F')).col1 FROM model", "SELECT filter(rs, rs -> rs.col1 = 'F')[1].col1 FROM model"},
                {"SELECT any(filter(rs, rs -> rs.col1 = 'F')).col1 AS a FROM model", "SELECT filter(rs, rs -> rs.col1 = 'F')[1].col1 AS a FROM model"},
                {"SELECT concat(any(filter(rs, rs -> rs.col1 = 'F')).col1, 'foo') AS a FROM model", "SELECT concat(filter(rs, rs -> rs.col1 = 'F')[1].col1, 'foo') AS a FROM model"},
        };
    }

    @Test(dataProvider = "rewrite")
    public void testSqlRewrite(String original, String expected)
    {
        assertThat(formatSql(ANY_FUNCTION_SQL_REWRITE.apply(parseSql(original), null, null)))
                .isEqualTo(formatSql(parseSql(expected)));
    }
}
