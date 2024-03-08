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

import io.accio.base.AccioMDL;
import io.accio.base.SessionContext;
import io.accio.base.config.BigQueryConfig;
import io.accio.base.config.ConfigManager;
import io.accio.base.dto.Manifest;
import io.accio.main.AccioMetastore;
import io.accio.main.connector.bigquery.BigQueryMetadata;
import io.accio.main.connector.bigquery.BigQuerySqlConverter;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Model.model;
import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQuerySqlConverter
{
    private static final SessionContext DEFAULT_SESSION_CONTEXT = SessionContext.builder()
            .setCatalog("canner-cml")
            .setSchema("tpch_tiny")
            .build();
    private BigQuerySqlConverter bigQuerySqlConverter;

    @BeforeClass
    public void createBigQueryClient()
    {
        BigQueryConfig config = new BigQueryConfig();
        config.setProjectId(getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .setParentProjectId(getenv("TEST_BIG_QUERY_PARENT_PROJECT_ID"))
                .setCredentialsKey(getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .setLocation("asia-east1");

        ConfigManager configManager = new ConfigManager(
                null,
                null,
                config,
                null,
                null,
                null);

        BigQueryMetadata bigQueryMetadata = new BigQueryMetadata(configManager);
        AccioMetastore accioMetastore = new AccioMetastore();
        accioMetastore.setAccioMDL(AccioMDL.fromManifest(Manifest.builder().setCatalog("canner-cml").setSchema("tpch_tiny")
                .setModels(List.of(model("Orders", "select * from \"canner-cml\".\"tpch_tiny\".\"orders\"",
                        List.of(column("custkey", "integer", null, true, "o_custkey"))))).build()), null);
        bigQuerySqlConverter = new BigQuerySqlConverter(bigQueryMetadata, accioMetastore);
    }

    @Test
    public void testBigQueryGroupByOrdinal()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT custkey, COUNT(*) AS cnt\n" +
                        "FROM \"Orders\"\n" +
                        "GROUP BY 1", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT\n" +
                        "  custkey\n" +
                        ", COUNT(*) cnt\n" +
                        "FROM\n" +
                        "  `Orders`\n" +
                        "GROUP BY 1\n");
    }

    @Test
    public void testCaseSensitive()
    {
        assertThat(bigQuerySqlConverter.convert("SELECT a FROM \"Orders\"", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT a\n" +
                        "FROM\n" +
                        "  `Orders`\n");
        assertThat(bigQuerySqlConverter.convert("SELECT b FROM \"Orders\"", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT b\n" +
                        "FROM\n" +
                        "  `Orders`\n");
    }

    @Test
    public void testDereferenceExpression()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT t.\"transform(Customer.orders, (orderItem) -> orderItem.orderstatus)\" from t", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT t.`transform(Customer.orders, (orderItem) -> orderItem.orderstatus)`\n" +
                        "FROM\n" +
                        "  t\n");
    }

    @Test
    public void testRemoveColumnAliasInAliasRelation()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT c_1, c_2\n" +
                        "FROM (SELECT c1, c2, c3 FROM \"graph-mdl\".\"test\".\"table\") AS t(c_1, c_2, c_3)", DEFAULT_SESSION_CONTEXT))
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
                        "FROM (SELECT c1, c2, c3 FROM \"graph-mdl\".\"test\".\"table\") AS t(c_1, c_2, c_3)", DEFAULT_SESSION_CONTEXT))
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
                        "FROM (SELECT canner.c1, canner.c2, canner.c3 FROM \"graph-mdl\".\"test\".\"table\") AS t(c_1, c_2, c_3)", DEFAULT_SESSION_CONTEXT))
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
                "WITH b(n) AS (SELECT name FROM Book) SELECT n FROM b", DEFAULT_SESSION_CONTEXT))
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
                        ") AS t (id, name)", DEFAULT_SESSION_CONTEXT))
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
        assertThat(bigQuerySqlConverter.convert("SELECT a.id FROM UNNEST(ARRAY[1]) as a(id)", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT id\n" +
                        "FROM\n" +
                        "  UNNEST(ARRAY[1]) id\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT a.id FROM (SELECT a.id FROM UNNEST(ARRAY[1]) as a(id)) a", DEFAULT_SESSION_CONTEXT))
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
                        "CROSS JOIN UNNEST(Sequences.some_numbers) AS u (uc) LEFT JOIN Sequences t on (u.uc = t.id)", DEFAULT_SESSION_CONTEXT))
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
                        "      WHERE c.oid = t.typrelid))", DEFAULT_SESSION_CONTEXT))
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
                        "    WHERE (r.regionkey = n.regionkey))", DEFAULT_SESSION_CONTEXT))
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
                        "ORDER BY test.t1.c2", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT\n" +
                        "  t1.c1\n" +
                        ", `t1`.c2\n" +
                        ", t1.c3\n" +
                        "FROM\n" +
                        "  accio.test.t1\n" +
                        "WHERE (t1.c1 = 1)\n" +
                        "ORDER BY t1.c2 ASC\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT accio.test.t1.c1, test2.t2.c1\n" +
                        "FROM accio.test.t1\n" +
                        "LEFT JOIN accio.test2.t2 on test.t1.c1 = test2.t2.c1", DEFAULT_SESSION_CONTEXT))
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
                        "GROUP BY (c1, c2, c3)", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT count(*)\n" +
                        "FROM\n" +
                        "  accio.test.t1\n" +
                        "GROUP BY c1, c2, c3\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT count(*)" +
                        "FROM accio.test.t1\n" +
                        "GROUP BY (c1, c2), c3", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT count(*)\n" +
                        "FROM\n" +
                        "  accio.test.t1\n" +
                        "GROUP BY c1, c2, c3\n");
    }

    @Test
    public void testRewriteNamesToAlias()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT FLOOR(o_orderkey) o_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  \"Orders\"\n" +
                        "GROUP BY FLOOR(o_orderkey)\n" +
                        "ORDER BY FLOOR(o_orderkey) ASC\n", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT\n" +
                        "  FLOOR(o_orderkey) o_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  `Orders`\n" +
                        "GROUP BY o_orderkey\n" +
                        "ORDER BY o_orderkey ASC\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT\n" +
                        "  FLOOR(o_orderkey) o_orderkey\n" +
                        ", COUNT(*) count\n" +
                        "FROM\n" +
                        "  \"Orders\"\n" +
                        "GROUP BY o_orderkey\n" +
                        "ORDER BY o_orderkey ASC\n", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT\n" +
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
        assertThat(bigQuerySqlConverter.convert(
                "SELECT TIMESTAMP '2023-07-04 09:41:43.805201' + INTERVAL '1 YEAR'", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT (CAST(TIMESTAMP '2023-07-04 09:41:43.805201' AS DATETIME) + INTERVAL '1' YEAR)\n\n");

        assertThat(bigQuerySqlConverter.convert(
                "SELECT DATE '2023-07-04' + INTERVAL '1 YEAR'", DEFAULT_SESSION_CONTEXT))
                .isEqualTo("SELECT (CAST('2023-07-04' AS DATE) + INTERVAL '1' YEAR)\n\n");
    }
}
