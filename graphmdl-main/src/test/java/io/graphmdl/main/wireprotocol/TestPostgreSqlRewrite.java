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

package io.graphmdl.main.wireprotocol;

import io.graphmdl.main.pgcatalog.regtype.RegObjectFactory;
import io.graphmdl.main.pgcatalog.regtype.TestingPgMetadata;
import io.graphmdl.main.sql.PostgreSqlRewrite;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.text.MessageFormat;

import static io.graphmdl.main.pgcatalog.OidHash.functionOid;
import static io.graphmdl.main.pgcatalog.OidHash.oid;
import static io.trino.execution.sql.SqlFormatterUtil.getFormattedSql;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPostgreSqlRewrite
{
    private static final String DEFAULT_CATALOG = "default";
    private static final String PGTYPE = "pg_type";
    private static final String PGCATALOG_PGTYPE = DEFAULT_CATALOG + ".pg_catalog.pg_type";
    private PostgreSqlRewrite postgreSqlRewrite;
    private SqlParser sqlParser;
    private RegObjectFactory regObjectFactory;

    @BeforeClass
    public void init()
    {
        postgreSqlRewrite = new PostgreSqlRewrite();
        sqlParser = new SqlParser();
        regObjectFactory = new RegObjectFactory(new TestingPgMetadata());
    }

    @Test
    public void testSelect()
    {
        assertNoRewrite("SELECT * FROM nopg_type");

        assertTableNameRewriteAndNoRewrite("SELECT * FROM {0}");
        assertTableNameRewriteAndNoRewrite("SELECT * FROM {0} WHERE {0}.oid = 1");
        assertTableNameRewriteAndNoRewrite("SELECT * FROM {0} a1 WHERE a1.oid = 1");
        assertTableNameRewriteAndNoRewrite("SELECT {0}.oid FROM {0}");
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertTableNameRewriteAndNoRewrite("CREATE TABLE t1 AS SELECT * FROM {0}");
    }

    @Test
    public void testDescribe()
    {
        assertTableNameRewriteAndNoRewrite("DESCRIBE {0}");
    }

    @Test
    public void testJoin()
    {
        assertTableNameRewriteAndNoRewrite("SELECT * FROM canner.s1.t1 JOIN {0} ON canner.s1.t1.c1 = {0}.oid");
        assertTableNameRewriteAndNoRewrite("SELECT * FROM canner.s1.t1 ali1 JOIN {0} ali2 ON al1.c1 = ali2.oid");
    }

    @Test
    public void testUnion()
    {
        assertTableNameRewriteAndNoRewrite("SELECT * FROM canner.s1.t1 UNION SELECT * FROM {0}");
    }

    @Test
    public void testSubQuery()
    {
        assertTableNameRewriteAndNoRewrite("SELECT oid FROM (SELECT * FROM {0})");
    }

    @Test
    public void testWithQuery()
    {
        assertTableNameRewriteAndNoRewrite("WITH table_with AS (SELECT * FROM {0}) SELECT * FROM table_with");
    }

    @Test
    public void testWithQueryTwoTable()
    {
        assertTableNameRewriteAndNoRewrite("WITH " +
                "table_with1 AS (SELECT * FROM c1)," +
                "table_with2 AS (SELECT * FROM {0}) " +
                "SELECT * FROM table_with1 JOIN table_with2 ON table_with1.key = table_with2.oid");
    }

    @Test
    public void testShowStats()
    {
        assertTableNameRewriteAndNoRewrite("SHOW STATS FOR {0}");
        assertTableNameRewriteAndNoRewrite("SHOW STATS FOR (SELECT * FROM {0} WHERE key > 1000)");
    }

    @Test
    public void testShowCreateTable()
    {
        assertTableNameRewriteAndNoRewrite("SHOW CREATE TABLE {0}");
    }

    @Test
    public void testShowColumns()
    {
        assertTableNameRewriteAndNoRewrite("SHOW COLUMNS FROM {0}");
    }

    @Test
    public void testInSubquery()
    {
        assertTableNameRewriteAndNoRewrite("SELECT * FROM t1 WHERE key IN (SELECT oid FROM {0})");
    }

    @Test
    public void testJoinUsing()
    {
        assertNoRewrite("select t1.c1, t1.c2, t2.c3 from (values(1,2), (2,3)) as t1(c1,c2) join (select * from (values(1,3), (2,4))) as t2(c1,c3) using (c1)");
    }

    @Test(enabled = false)
    public void testFunction()
    {
        // TODO: support pg function
        assertRewrite("select pg_catalog.current_schema(true)", "select current_schema(true) current_schema");
        assertRewrite("select pg_catalog.pg_get_expr('', 1)", "select pg_get_expr('', 1) pg_get_expr");
        assertRewrite("select pg_catalog.array_upper(array[array[1,2,3], array[2,3,4]], 1)", "select array_upper(array[array[1,2,3], array[2,3,4]], 1) array_upper");
        assertRewrite("select pg_catalog.array_lower(array[array[1,2,3], array[2,3,4]], 1)", "select array_lower(array[array[1,2,3], array[2,3,4]], 1) array_lower");
        // TODO
        // assertRewrite(format("select pg_catalog.pg_get_userbyid(%d)", WIRE_PROTOCOL_USER_OID), format("select pg_get_userbyid(%d) pg_get_userbyid", WIRE_PROTOCOL_USER_OID));
        assertRewrite("select pg_catalog.pg_get_keywords()", "select pg_get_keywords() pg_get_keywords");
        assertRewrite("select information_schema._pg_expandarray(ARRAY ['a','b'])", "select _pg_expandarray(ARRAY ['a','b']) _pg_expandarray");
        assertRewrite("select (information_schema._pg_expandarray(ARRAY ['a','b'])).n", "select _pg_expandarray(ARRAY ['a','b']).n n");
        assertRewrite("select (information_schema._pg_expandarray(ARRAY ['a','b']))", "select _pg_expandarray(ARRAY ['a','b']) _pg_expandarray");
        assertRewrite("select version()", "select pg_version() version");
        assertRewrite("select session_user", "select \"$current_user\"() session_user");
        assertRewrite("select user", "select \"$current_user\"() user");
        assertRewrite("select to_date()", "select pg_to_date() to_date");
    }

    @Test
    public void testGroupByColumnAlias()
    {
        assertRewrite("select concat('x', c1) as x, count(1) as y from (values (1, 2, 3), (4, 5, 6)) as t1(c1, c2, c3) group by x",
                "select concat('x', c1) as x, count(1) as y from (values (1, 2, 3), (4, 5, 6)) as t1(c1, c2, c3) group by concat('x', c1)");
        assertRewrite("select c1 as x, sum(c2) as y from (values (1, 2, 3), (4, 5, 6)) as t1(c1, c2, c3) group by x",
                "select c1 as x, sum(c2) as y from (values (1, 2, 3), (4, 5, 6)) as t1(c1, c2, c3) group by c1");
        assertRewrite("select c1 as x, c2, sum(c2) as y from (values (1, 2, 3), (4, 5, 6)) as t1(c1, c2, c3) group by x, c2",
                "select c1 as x, c2, sum(c2) as y from (values (1, 2, 3), (4, 5, 6)) as t1(c1, c2, c3) group by c1, c2");
        assertRewrite("select concat('x', c1) as \"x\", count(1) as y from (values (1, 2, 3), (4, 5, 6)) as t1(c1, c2, c3) group by \"x\"",
                "select concat('x', c1) as \"x\", count(1) as y from (values (1, 2, 3), (4, 5, 6)) as t1(c1, c2, c3) group by concat('x', c1)");
        assertRewrite("WITH\n" +
                "  Orders AS (\n" +
                "   SELECT\n" +
                "     o_orderkey orderkey\n" +
                "   , o_custkey custkey\n" +
                "   , o_totalprice totalprice\n" +
                "   FROM\n" +
                "     (\n" +
                "      VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1)\n" +
                "   ) t(o_orderkey, o_custkey, o_totalprice)\n" +
                ") \n" +
                ", Revenue AS (\n" +
                "   SELECT\n" +
                "     custkey o_custkey\n" +
                "   , sum(totalprice) revenue\n" +
                "   FROM\n" +
                "     Orders\n" +
                "   GROUP BY o_custkey\n" +
                ") \n" +
                "SELECT\n" +
                "  o_custkey\n" +
                ", revenue\n" +
                "FROM\n" +
                "  Revenue", "WITH\n" +
                "  Orders AS (\n" +
                "   SELECT\n" +
                "     o_orderkey orderkey\n" +
                "   , o_custkey custkey\n" +
                "   , o_totalprice totalprice\n" +
                "   FROM\n" +
                "     (\n" +
                " VALUES \n" +
                "        ROW (1, 1, 1)\n" +
                "      , ROW (2, 2, 1)\n" +
                "      , ROW (3, 3, 1)\n" +
                "   )  t (o_orderkey, o_custkey, o_totalprice)\n" +
                ") \n" +
                ", Revenue AS (\n" +
                "   SELECT\n" +
                "     custkey o_custkey\n" +
                "   , sum(totalprice) revenue\n" +
                "   FROM\n" +
                "     Orders\n" +
                "   GROUP BY custkey\n" +
                ") \n" +
                "SELECT\n" +
                "  o_custkey\n" +
                ", revenue\n" +
                "FROM\n" +
                "  Revenue");

        assertNoRewrite("select c1, sum(c2) as y from (values(1, 2), (3, 5)) as t1(c1,c2) group by c1");
        assertNoRewrite("WITH\n" +
                "  Orders AS (\n" +
                "   SELECT\n" +
                "     o_orderkey orderkey\n" +
                "   , o_custkey custkey\n" +
                "   , o_totalprice totalprice\n" +
                "   FROM\n" +
                "     (\n" +
                "      VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1)\n" +
                "   ) t(o_orderkey, o_custkey, o_totalprice)\n" +
                ") \n" +
                ", Revenue AS (\n" +
                "   SELECT\n" +
                "     custkey\n" +
                "   , sum(totalprice) revenue\n" +
                "   FROM\n" +
                "     Orders\n" +
                "   GROUP BY custkey\n" +
                ") \n" +
                "SELECT\n" +
                "  custkey\n" +
                ", revenue\n" +
                "FROM\n" +
                "  Revenue");
    }

    @Test
    public void testIntervalLiteralWithInternalField()
    {
        assertRewrite("select CAST((CAST(now() AS timestamp) + (INTERVAL '-30 day')) AS date)", "select CAST((CAST(now() AS timestamp) + (INTERVAL - '30' day)) AS date)");
        assertRewrite("select CAST((CAST(now() AS timestamp) + (INTERVAL '30 month')) AS date)", "select CAST((CAST(now() AS timestamp) + (INTERVAL + '30' month)) AS date)");
        assertRewrite("select CAST((CAST(now() AS timestamp) + (INTERVAL '+30 month')) AS date)", "select CAST((CAST(now() AS timestamp) + (INTERVAL + '30' month)) AS date)");
    }

    @Test
    public void testRewriteLikePredicate()
    {
        assertNoRewrite("SELECT 'South_America' LIKE 'South&_America' ESCAPE '&' AS Continent");

        assertRewrite("SELECT 'South_America' LIKE 'South\\_America' AS Continent", "SELECT 'South_America' LIKE 'South\\_America' ESCAPE '\\' AS Continent");
        assertRewrite("SELECT 'South_America' NOT LIKE 'South\\_America' AS Continent", "SELECT 'South_America' NOT LIKE 'South\\_America' ESCAPE '\\' AS Continent");
    }

    @Test
    public void testOidOperator()
    {
        assertRewrite("SELECT * FROM pg_type WHERE typinput = 'array_in'::regproc",
                format("SELECT * FROM %s.pg_catalog.pg_type WHERE typinput = %s", DEFAULT_CATALOG, functionOid("array_in")));
        assertRewrite("SELECT * FROM pg_attribute WHERE attrelid = 't1'::regclass",
                format("SELECT * FROM %s.pg_catalog.pg_attribute WHERE attrelid = %s", DEFAULT_CATALOG, oid("t1")));
        assertRewrite("SELECT typinput = 'array_in'::regproc FROM pg_type ",
                format("SELECT typinput = %s FROM %s.pg_catalog.pg_type", functionOid("array_in"), DEFAULT_CATALOG));
        assertRewrite(format("SELECT typinput = %s::regproc FROM pg_type ", functionOid("array_in")),
                format("SELECT typinput = %s FROM %s.pg_catalog.pg_type", functionOid("array_in"), DEFAULT_CATALOG));
        assertRewrite(format("SELECT attrelid = %s::regclass FROM pg_attribute ", oid("t1")),
                format("SELECT attrelid = %s FROM %s.pg_catalog.pg_attribute", oid("t1"), DEFAULT_CATALOG));

        assertRewrite(format("SELECT * FROM pg_type WHERE typinput = %s::regproc", functionOid("array_in")),
                format("SELECT * FROM %s.pg_catalog.pg_type WHERE typinput = %s", DEFAULT_CATALOG, functionOid("array_in")));
        assertRewrite(format("SELECT * FROM pg_attribute WHERE attrelid = %s::regclass", oid("t1")),
                format("SELECT * FROM %s.pg_catalog.pg_attribute WHERE attrelid = %s", DEFAULT_CATALOG, oid("t1")));

        assertRewrite("SELECT 'array_in'::regproc", "SELECT 'array_in'");
        assertRewrite("SELECT 't1'::regclass", "SELECT 't1'");
        assertRewrite(format("SELECT %s::regproc", functionOid("array_in")), "SELECT 'array_in'");
        assertRewrite(format("SELECT %s::regclass", oid("t1")), "SELECT 't1'");
    }

    void assertTableNameRewriteAndNoRewrite(@Language("SQL") String sqlFormat)
    {
        // sql submitted by pg jdbc will only like `pg_type` and `pg_catalog.pg_type`.
        assertRewrite(MessageFormat.format(sqlFormat, PGTYPE), MessageFormat.format(sqlFormat, PGCATALOG_PGTYPE));
    }

    private void assertRewrite(
            @Language("SQL") String sql,
            @Language("SQL") String expectSql)
    {
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DOUBLE));
        Statement result = postgreSqlRewrite.rewrite(regObjectFactory, DEFAULT_CATALOG, statement);
        Statement expect = sqlParser.createStatement(expectSql, new ParsingOptions(AS_DOUBLE));
        assertThat(getFormattedSql(result, sqlParser)).isEqualTo(getFormattedSql(expect, sqlParser));
        assertThat(result).isEqualTo(expect);
    }

    private void assertNoRewrite(@Language("SQL") String sql)
    {
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DOUBLE));

        Statement result = postgreSqlRewrite.rewrite(regObjectFactory, DEFAULT_CATALOG, statement);

        assertThat(result).describedAs("%n[actual]%n%s[expect]%n%s", getFormattedSql(result, sqlParser), getFormattedSql(statement, sqlParser)).isEqualTo(statement);
    }
}
