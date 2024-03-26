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

package io.wren.sqlglot.converter;

import io.wren.base.SessionContext;
import io.wren.sqlglot.TestingSQLGlotServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.wren.sqlglot.glot.SQLGlot.Dialect.BIGQUERY;
import static io.wren.sqlglot.glot.SQLGlot.Dialect.DUCKDB;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSQLGlotConverter
{
    private static final SessionContext DEFAULT_SESSION_CONTEXT = SessionContext.builder().build();
    private TestingSQLGlotServer testingSQLGlotServer;

    @BeforeClass
    public void setup()
    {
        testingSQLGlotServer = new TestingSQLGlotServer();
    }

    @AfterClass
    public void close()
    {
        testingSQLGlotServer.close();
    }

    @Test
    public void testGenerateArray()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setReadDialect(BIGQUERY)
                .setWriteDialect(DUCKDB)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT generate_array(1, 10)",
                "SELECT GENERATE_SERIES(1, 10)");
    }

    @Test
    public void testSubstring()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT substring('Thomas' from 2 for 3)",
                "SELECT SUBSTRING('Thomas', 2, 3)");
    }

    @Test
    public void testArray()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(DUCKDB)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT ARRAY[1,2,3][1]",
                "SELECT ([1, 2, 3])[1]");
    }

    @Test
    public void testReplaceColumnAliasInUnnest()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT a.id FROM UNNEST(ARRAY[1]) as a(id)",
                "SELECT id FROM UNNEST([1]) AS id");

        assertConvert(sqlGlotConverter,
                "SELECT a.id FROM (SELECT a.id FROM UNNEST(ARRAY[1]) as a(id)) a",
                "SELECT a.id FROM (SELECT id FROM UNNEST([1]) AS id) AS a");
    }

    @Test(description = "SQLGlot transpiled sql can't run in BigQuery")
    public void testReplaceColumnAliasInUnnest2()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "WITH Sequences(id, some_numbers) AS (VALUES (1, ARRAY[0, 1, 1, 2, 3, 5]), (2, ARRAY[2, 4, 8, 16, 32]), (3, ARRAY[5, 10])) " +
                        "SELECT u.uc FROM Sequences CROSS JOIN UNNEST(Sequences.some_numbers) AS u (uc) LEFT JOIN Sequences t on (u.uc = t.id)",
                "WITH Sequences AS (SELECT 1 id, ARRAY[0,1,1,2,3,5] some_numbers UNION ALL SELECT 2 id, ARRAY[2,4,8,16,32] some_numbers UNION ALL SELECT 3 id, ARRAY[5,10] some_numbers) " +
                        "SELECT uc FROM ((Sequences CROSS JOIN UNNEST(Sequences.some_numbers) uc) LEFT JOIN Sequences t ON (uc = t.id))");

        // SQLGlot transpiled sql
        // WITH Sequences AS (VALUES (1, [0, 1, 1, 2, 3, 5]), (2, [2, 4, 8, 16, 32]), (3, [5, 10])) SELECT uc FROM Sequences CROSS JOIN UNNEST(Sequences.some_numbers) AS uc LEFT JOIN Sequences AS t ON (uc = t.id)
    }

    @Test(description = "SQLGlot unsupported feature")
    public void testTransformCorrelatedJoinToJoin()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT t.typname, t.oid\n" +
                        "FROM pg_type AS t\n" +
                        "  INNER JOIN pg_namespace AS n ON t.typnamespace = n.oid\n" +
                        "WHERE n.nspname <> 'pg_toast'\n" +
                        "  AND (t.typrelid = 0\n" +
                        "  OR (SELECT c.relkind = 'c'\n" +
                        "      FROM pg_class AS c\n" +
                        "      WHERE c.oid = t.typrelid))",
                "SELECT\n" +
                        "  t.typname\n" +
                        ", t.oid\n" +
                        "FROM\n" +
                        "  ((pg_type t\n" +
                        "INNER JOIN pg_namespace n ON (t.typnamespace = n.oid))\n" +
                        "LEFT JOIN pg_class c ON (c.oid = t.typrelid))\n" +
                        "WHERE ((n.nspname <> 'pg_toast') AND ((t.typrelid = 0) OR (c.relkind = 'c')))\n");

        assertConvert(sqlGlotConverter,
                "SELECT n.nationkey, n.name\n" +
                        "FROM nation n\n" +
                        "WHERE \n" +
                        "  (SELECT (r.regionkey = 1)\n" +
                        "    FROM region r\n" +
                        "    WHERE (r.regionkey = n.regionkey))",
                "SELECT\n" +
                        "  n.nationkey\n" +
                        ", n.name\n" +
                        "FROM\n" +
                        "  (nation n\n" +
                        "LEFT JOIN region r ON (r.regionkey = n.regionkey))\n" +
                        "WHERE (r.regionkey = 1)\n");
    }

    @Test(description = "SQLGlot unsupported feature")
    public void testRemoveCatalogSchemaColumnPrefix()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT \"wren\".test.t1.c1, test.\"t1\".c2, t1.c3\n" +
                        "FROM wren.test.t1\n" +
                        "WHERE \"wren\".test.t1.c1 = 1\n" +
                        "ORDER BY test.t1.c2",
                "SELECT\n" +
                        "  t1.c1\n" +
                        ", `t1`.c2\n" +
                        ", t1.c3\n" +
                        "FROM\n" +
                        "  wren.test.t1\n" +
                        "WHERE (t1.c1 = 1)\n" +
                        "ORDER BY t1.c2 ASC\n");

        assertConvert(sqlGlotConverter,
                "SELECT wren.test.t1.c1, test2.t2.c1\n" +
                        "FROM wren.test.t1\n" +
                        "LEFT JOIN wren.test2.t2 on test.t1.c1 = test2.t2.c1",
                "SELECT\n" +
                        "  t1.c1\n" +
                        ", t2.c1\n" +
                        "FROM\n" +
                        "  (wren.test.t1\n" +
                        "LEFT JOIN wren.test2.t2 ON (t1.c1 = t2.c1))\n");
    }

    @Test(description = "SQLGlot unsupported feature")
    public void testFlattenGroupingElements()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT count(*)\n" +
                        "FROM wren.test.t1\n" +
                        "GROUP BY (c1, c2, c3)",
                "SELECT count(*)\n" +
                        "FROM\n" +
                        "  wren.test.t1\n" +
                        "GROUP BY c1, c2, c3\n");

        assertConvert(sqlGlotConverter,
                "SELECT count(*)\n" +
                        "FROM wren.test.t1\n" +
                        "GROUP BY (c1, c2), c3",
                "SELECT count(*)\n" +
                        "FROM\n" +
                        "  wren.test.t1\n" +
                        "GROUP BY c1, c2, c3\n");
    }

    @Test(description = "Don't understand the purpose of this test")
    public void testRewriteNamesToAlias()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT\n" +
                        "  FLOOR(o_orderkey) o_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  \"Orders\"\n" +
                        "GROUP BY FLOOR(o_orderkey)\n" +
                        "ORDER BY FLOOR(o_orderkey) ASC\n",
                "SELECT\n" +
                        "  FLOOR(o_orderkey) o_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  `Orders`\n" +
                        "GROUP BY o_orderkey\n" +
                        "ORDER BY o_orderkey ASC\n");

        assertConvert(sqlGlotConverter,
                "SELECT\n" +
                        "  FLOOR(o_orderkey) o_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  \"Orders\"\n" +
                        "GROUP BY o_orderkey\n" +
                        "ORDER BY o_orderkey ASC\n",
                "SELECT\n" +
                        "  FLOOR(o_orderkey) o_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  `Orders`\n" +
                        "GROUP BY o_orderkey\n" +
                        "ORDER BY o_orderkey ASC\n");
    }

    @Test
    public void testRewriteArithemetic()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT TIMESTAMP '2023-07-04 09:41:43.805201' + INTERVAL '1 YEAR'",
                "SELECT CAST('2023-07-04 09:41:43.805201' AS DATETIME) + INTERVAL '1' YEAR");

        assertConvert(sqlGlotConverter,
                "SELECT DATE '2023-07-04' + INTERVAL '1 YEAR'",
                "SELECT CAST('2023-07-04' AS DATE) + INTERVAL '1' YEAR");
    }

    @Test
    public void testBigQueryGroupByOrdinal()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT custkey, COUNT(*) AS cnt FROM \"Order\" GROUP BY 1",
                "SELECT custkey, COUNT(*) AS cnt FROM `Order` GROUP BY 1");
    }

    @Test
    public void testDereferenceExpression()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT t.\"transform(Customer.orders, (orderItem) -> orderItem.orderstatus)\" from t",
                "SELECT t.`transform(Customer.orders, (orderItem) -> orderItem.orderstatus)` FROM t");
    }

    @Test(enabled = false, description = "SQLGlot does not support this feature, message(Named columns are not supported in table alias) from SQLGlot")
    public void testRemoveColumnAliasInAliasRelation()
    {
        SQLGlotConverter sqlGlotConverter = SQLGlotConverter.builder()
                .setWriteDialect(BIGQUERY)
                .build();

        assertConvert(sqlGlotConverter,
                "SELECT c_1, c_2 FROM (SELECT c1, c2, c3 FROM \"graph-mdl\".\"test\".\"table\") AS t(c_1, c_2, c_3)",
                "SELECT c_1, c_2 FROM (SELECT c1 c_1, c2 c_2, c3 c_3 FROM `graph-mdl`.`test`.`table`) t");

        // SQLGlot transpiled sql
        // SELECT c_1, c_2 FROM (SELECT c1, c2, c3 FROM `graph-mdl`.`test`.`table`) AS t
    }

    private static void assertConvert(SQLGlotConverter sqlGlotConverter, String sql, String expected)
    {
        assertThat(sqlGlotConverter.convert(sql, DEFAULT_SESSION_CONTEXT)).isEqualTo(expected);
    }
}
