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

package io.accio;

import io.accio.base.SessionContext;
import io.accio.connector.bigquery.BigQueryClient;
import io.accio.main.connector.bigquery.BigQueryConfig;
import io.accio.main.connector.bigquery.BigQueryMetadata;
import io.accio.main.connector.bigquery.BigQuerySqlConverter;
import io.accio.main.server.module.BigQueryConnectorModule;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQuerySqlConverter
{
    private BigQuerySqlConverter bigQuerySqlConverter;

    @BeforeClass
    public void createBigQueryClient()
    {
        BigQueryConfig config = new BigQueryConfig();
        config.setProjectId(getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .setParentProjectId(getenv("TEST_BIG_QUERY_PARENT_PROJECT_ID"))
                .setCredentialsKey(getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .setLocation("asia-east1");

        BigQueryClient bigQueryClient = BigQueryConnectorModule.provideBigQuery(
                config,
                BigQueryConnectorModule.createHeaderProvider(),
                BigQueryConnectorModule.provideBigQueryCredentialsSupplier(config));

        BigQueryMetadata bigQueryMetadata = new BigQueryMetadata(bigQueryClient, config);
        bigQuerySqlConverter = new BigQuerySqlConverter(bigQueryMetadata);
    }

    @Test
    public void testBigQueryGroupByOrdinal()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT o_custkey, COUNT(*) AS cnt\n" +
                        "FROM \"canner-cml\".\"tpch_tiny\".\"orders\"\n" +
                        "GROUP BY 1", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  o_custkey\n" +
                        ", COUNT(*) cnt\n" +
                        "FROM\n" +
                        "  `canner-cml`.`tpch_tiny`.`orders`\n" +
                        "GROUP BY 1\n");
    }

    @Test
    public void testCaseSensitive()
    {
        assertThat(bigQuerySqlConverter.convert("SELECT a FROM \"canner-cml\".\"cml_temp\".\"canner\"", SessionContext.builder().build()))
                .isEqualTo("SELECT a\n" +
                        "FROM\n" +
                        "  `canner-cml`.`cml_temp`.`canner`\n");
        assertThat(bigQuerySqlConverter.convert("SELECT b FROM \"canner-cml\".\"cml_temp\".\"CANNER\"", SessionContext.builder().build()))
                .isEqualTo("SELECT b\n" +
                        "FROM\n" +
                        "  `canner-cml`.`cml_temp`.`CANNER`\n");
    }

    @Test
    public void testDereferenceExpression()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT t.\"transform(Customer.orders, (orderItem) -> orderItem.orderstatus)\" from t", SessionContext.builder().build()))
                .isEqualTo("SELECT t.`transform(Customer.orders, (orderItem) -> orderItem.orderstatus)`\n" +
                        "FROM\n" +
                        "  t\n");
    }

    @Test
    public void testRemoveColumnAliasInAliasRelation()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT c_1, c_2\n" +
                        "FROM (SELECT c1, c2, c3 FROM \"graph-mdl\".\"test\".\"table\") AS t(c_1, c_2, c_3)", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  c_1\n" +
                        ", c_2\n" +
                        "FROM\n" +
                        "  (\n" +
                        "   SELECT\n" +
                        "     c1 c_1\n" +
                        "   , c2 c_2\n" +
                        "   , c3 c_3\n" +
                        "   FROM\n" +
                        "     `graph-mdl`.`test`.`table`\n" +
                        ")  t\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT t.c_1, t.c_2\n" +
                        "FROM (SELECT c1, c2, c3 FROM \"graph-mdl\".\"test\".\"table\") AS t(c_1, c_2, c_3)", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  t.c_1\n" +
                        ", t.c_2\n" +
                        "FROM\n" +
                        "  (\n" +
                        "   SELECT\n" +
                        "     c1 c_1\n" +
                        "   , c2 c_2\n" +
                        "   , c3 c_3\n" +
                        "   FROM\n" +
                        "     `graph-mdl`.`test`.`table`\n" +
                        ")  t\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT c_1, c_2\n" +
                        "FROM (SELECT canner.c1, canner.c2, canner.c3 FROM \"graph-mdl\".\"test\".\"table\") AS t(c_1, c_2, c_3)", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  c_1\n" +
                        ", c_2\n" +
                        "FROM\n" +
                        "  (\n" +
                        "   SELECT\n" +
                        "     canner.c1 c_1\n" +
                        "   , canner.c2 c_2\n" +
                        "   , canner.c3 c_3\n" +
                        "   FROM\n" +
                        "     `graph-mdl`.`test`.`table`\n" +
                        ")  t\n");

        assertThat(bigQuerySqlConverter.convert(
                "WITH b(n) AS (SELECT name FROM Book) SELECT n FROM b", SessionContext.builder().build()))
                .isEqualTo("WITH\n" +
                        "  b AS (\n" +
                        "   SELECT name n\n" +
                        "   FROM\n" +
                        "     Book\n" +
                        ") \n" +
                        "SELECT n\n" +
                        "FROM\n" +
                        "  b\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT * FROM (\n" +
                        "    VALUES\n" +
                        "        (1, 'a'),\n" +
                        "        (2, 'b'),\n" +
                        "        (3, 'c')\n" +
                        ") AS t (id, name)", SessionContext.builder().build()))
                .isEqualTo("SELECT *\n" +
                        "FROM\n" +
                        "  (\n" +
                        "   SELECT\n" +
                        "     1 id\n" +
                        "   , 'a' name\n" +
                        "\n" +
                        "UNION ALL    SELECT\n" +
                        "     2 id\n" +
                        "   , 'b' name\n" +
                        "\n" +
                        "UNION ALL    SELECT\n" +
                        "     3 id\n" +
                        "   , 'c' name\n" +
                        "\n" +
                        ")  t\n");
    }

    @Test
    public void testReplaceColumnAliasInUnnest()
    {
        assertThat(bigQuerySqlConverter.convert("SELECT a.id FROM UNNEST(ARRAY[1]) as a(id)", SessionContext.builder().build()))
                .isEqualTo("SELECT id\n" +
                        "FROM\n" +
                        "  UNNEST(ARRAY[1]) id\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT a.id FROM (SELECT a.id FROM UNNEST(ARRAY[1]) as a(id)) a", SessionContext.builder().build()))
                .isEqualTo("SELECT a.id\n" +
                        "FROM\n" +
                        "  (\n" +
                        "   SELECT id\n" +
                        "   FROM\n" +
                        "     UNNEST(ARRAY[1]) id\n" +
                        ")  a\n");

        assertThat(bigQuerySqlConverter.convert(
                "WITH Sequences(id, some_numbers) AS\n" +
                        "    (VALUES\n" +
                        "        (1, ARRAY[0, 1, 1, 2, 3, 5]),\n" +
                        "        (2, ARRAY[2, 4, 8, 16, 32]),\n" +
                        "        (3, ARRAY[5, 10])\n" +
                        "    )\n" +
                        "SELECT u.uc\n" +
                        "FROM Sequences\n" +
                        "CROSS JOIN UNNEST(Sequences.some_numbers) AS u (uc) LEFT JOIN Sequences t on (u.uc = t.id)", SessionContext.builder().build()))
                .isEqualTo("WITH\n" +
                        "  Sequences AS (\n" +
                        "   SELECT\n" +
                        "     1 id\n" +
                        "   , ARRAY[0,1,1,2,3,5] some_numbers\n" +
                        "\n" +
                        "UNION ALL    SELECT\n" +
                        "     2 id\n" +
                        "   , ARRAY[2,4,8,16,32] some_numbers\n" +
                        "\n" +
                        "UNION ALL    SELECT\n" +
                        "     3 id\n" +
                        "   , ARRAY[5,10] some_numbers\n" +
                        "\n" +
                        ") \n" +
                        "SELECT uc\n" +
                        "FROM\n" +
                        "  ((Sequences\n" +
                        "CROSS JOIN UNNEST(Sequences.some_numbers) uc)\n" +
                        "LEFT JOIN Sequences t ON (uc = t.id))\n");
    }

    @Test
    public void testTransformCorrelatedJoinToJoin()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT t.typname, t.oid\n" +
                        "FROM pg_type AS t\n" +
                        "  INNER JOIN pg_namespace AS n ON t.typnamespace = n.oid\n" +
                        "WHERE n.nspname <> 'pg_toast'\n" +
                        "  AND (t.typrelid = 0\n" +
                        "  OR (SELECT c.relkind = 'c'\n" +
                        "      FROM pg_class AS c\n" +
                        "      WHERE c.oid = t.typrelid))", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  t.typname\n" +
                        ", t.oid\n" +
                        "FROM\n" +
                        "  ((pg_type t\n" +
                        "INNER JOIN pg_namespace n ON (t.typnamespace = n.oid))\n" +
                        "LEFT JOIN pg_class c ON (c.oid = t.typrelid))\n" +
                        "WHERE ((n.nspname <> 'pg_toast') AND ((t.typrelid = 0) OR (c.relkind = 'c')))\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT n.nationkey, n.name\n" +
                        "FROM nation n\n" +
                        "WHERE \n" +
                        "  (SELECT (r.regionkey = 1)\n" +
                        "    FROM region r\n" +
                        "    WHERE (r.regionkey = n.regionkey))", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  n.nationkey\n" +
                        ", n.name\n" +
                        "FROM\n" +
                        "  (nation n\n" +
                        "LEFT JOIN region r ON (r.regionkey = n.regionkey))\n" +
                        "WHERE (r.regionkey = 1)\n");
    }

    @Test
    public void testRemoveCatalogSchemaColumnPrefix()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT \"accio\".test.t1.c1, test.\"t1\".c2, t1.c3\n" +
                        "FROM accio.test.t1\n" +
                        "WHERE \"accio\".test.t1.c1 = 1\n" +
                        "ORDER BY test.t1.c2", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  t1.c1\n" +
                        ", t1.c2\n" +
                        ", t1.c3\n" +
                        "FROM\n" +
                        "  accio.test.t1\n" +
                        "WHERE (t1.c1 = 1)\n" +
                        "ORDER BY t1.c2 ASC\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT accio.test.t1.c1, test2.t2.c1\n" +
                        "FROM accio.test.t1\n" +
                        "LEFT JOIN accio.test2.t2 on test.t1.c1 = test2.t2.c1", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  t1.c1\n" +
                        ", t2.c1\n" +
                        "FROM\n" +
                        "  (accio.test.t1\n" +
                        "LEFT JOIN accio.test2.t2 ON (t1.c1 = t2.c1))\n");
    }

    @Test
    public void testFlattenGroupingElements()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT count(*)" +
                        "FROM accio.test.t1\n" +
                        "GROUP BY (c1, c2, c3)", SessionContext.builder().build()))
                .isEqualTo("SELECT count(*)\n" +
                        "FROM\n" +
                        "  accio.test.t1\n" +
                        "GROUP BY c1, c2, c3\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT count(*)" +
                        "FROM accio.test.t1\n" +
                        "GROUP BY (c1, c2), c3", SessionContext.builder().build()))
                .isEqualTo("SELECT count(*)\n" +
                        "FROM\n" +
                        "  accio.test.t1\n" +
                        "GROUP BY c1, c2, c3\n");
    }

    @Test
    public void testRewriteNamesToAlias()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT FLOOR(l_orderkey) l_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  tpch_tiny.lineitem\n" +
                        "GROUP BY FLOOR(l_orderkey)\n" +
                        "ORDER BY FLOOR(l_orderkey) ASC\n", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  FLOOR(l_orderkey) l_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  tpch_tiny.lineitem\n" +
                        "GROUP BY l_orderkey\n" +
                        "ORDER BY l_orderkey ASC\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT\n" +
                        "  FLOOR(l_orderkey) l_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  tpch_tiny.lineitem\n" +
                        "GROUP BY l_orderkey\n" +
                        "ORDER BY l_orderkey ASC\n", SessionContext.builder().build()))
                .isEqualTo("SELECT\n" +
                        "  FLOOR(l_orderkey) l_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  tpch_tiny.lineitem\n" +
                        "GROUP BY l_orderkey\n" +
                        "ORDER BY l_orderkey ASC\n");
    }

    @Test
    public void testRewriteArithemetic()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT TIMESTAMP '2023-07-04 09:41:43.805201' + INTERVAL '1 YEAR'", SessionContext.builder().build()))
                .isEqualTo("SELECT (CAST(TIMESTAMP '2023-07-04 09:41:43.805201' AS DATETIME) + INTERVAL '1' YEAR)\n\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT DATE '2023-07-04' + INTERVAL '1 YEAR'", SessionContext.builder().build()))
                .isEqualTo("SELECT (CAST('2023-07-04' AS DATE) + INTERVAL '1' YEAR)\n\n");
    }
}
