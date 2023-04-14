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
package io.graphmdl;

import com.google.common.collect.ImmutableMap;
import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.SessionContext;
import io.graphmdl.sqlrewrite.PreAggregationRewrite;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPreAggregationRewrite
{
    private SqlParser sqlParser;

    private static final Map<CatalogSchemaTableName, String> PRE_AGG =
            ImmutableMap.<CatalogSchemaTableName, String>builder()
                    .put(new CatalogSchemaTableName("graphmdl", "w1", "t1"), "table_t1")
                    .put(new CatalogSchemaTableName("graphmdl", "w1", "t2"), "table_t2")
                    .put(new CatalogSchemaTableName("graphmdl", "tpch", "lineitem"), "table_lineitem")
                    .put(new CatalogSchemaTableName("graphmdl", "tpch", "part"), "table_part")
                    .put(new CatalogSchemaTableName("graphmdl", "tpch", "supplier"), "table_supplier")
                    .put(new CatalogSchemaTableName("graphmdl", "tpch", "partsupp"), "table_partsupp")
                    .put(new CatalogSchemaTableName("graphmdl", "tpch", "nation"), "table_nation")
                    .put(new CatalogSchemaTableName("graphmdl", "tpch", "region"), "table_region")
                    .put(new CatalogSchemaTableName("graphmdl", "tpch", "customer"), "table_customer")
                    .put(new CatalogSchemaTableName("graphmdl", "tpch", "orders"), "table_orders")
                    .put(new CatalogSchemaTableName("graphmdl", "escape", "t-1"), "table_t-1")
                    .build();

    @BeforeClass
    public void init()
    {
        sqlParser = new SqlParser();
    }

    @DataProvider(name = "oneTableProvider")
    public Object[][] oneTableProvider()
    {
        return new Object[][] {
                {OneTableTestData.create(null, null, "graphmdl.w1.t1")},
                {OneTableTestData.create("graphmdl", null, "graphmdl.w1.t1")},
                {OneTableTestData.create("graphmdl", null, "w1.t1")},
                {OneTableTestData.create("other", null, "graphmdl.w1.t1")},
                {OneTableTestData.create("graphmdl", "w1", "graphmdl.w1.t1")},
                {OneTableTestData.create("graphmdl", "w1", "w1.t1")},
                {OneTableTestData.create("graphmdl", "w1", "t1")},
                {OneTableTestData.create("graphmdl", "w2", "graphmdl.w1.t1")},
                {OneTableTestData.create("graphmdl", "w2", "w1.t1")},
                {OneTableTestData.create("other", "w1", "graphmdl.w1.t1")},
                {OneTableTestData.create("other", "w2", "graphmdl.w1.t1")},
        };
    }

    @DataProvider(name = "twoTableProvider")
    public Object[][] twoTableProvider()
    {
        return new Object[][] {
                {TwoTableTestData.create(null, null, "graphmdl.w1.t1", "graphmdl.w1.t2")},
                {TwoTableTestData.create("graphmdl", null, "graphmdl.w1.t1", "graphmdl.w1.t2")},
                {TwoTableTestData.create("graphmdl", null, "graphmdl.w1.t1", "w1.t2")},
                {TwoTableTestData.create("graphmdl", null, "w1.t1", "w1.t2")},
                {TwoTableTestData.create("graphmdl", "w1", "graphmdl.w1.t1", "graphmdl.w1.t2")},
                {TwoTableTestData.create("graphmdl", "w1", "graphmdl.w1.t1", "w1.t2")},
                {TwoTableTestData.create("graphmdl", "w1", "w1.t1", "w1.t2")},
                {TwoTableTestData.create("graphmdl", "w1", "t1", "t2")},
        };
    }

    @Test(dataProvider = "oneTableProvider")
    public void testSelect(OneTableTestData testData)
    {
        assertOneTable("SELECT * FROM {0}", testData);
    }

    @Test(dataProvider = "twoTableProvider")
    public void testJoin(TwoTableTestData testData)
    {
        assertTwoTables("SELECT * FROM {0} a LEFT JOIN {1} b ON a.key = b.key", testData);
    }

    @Test
    public void testJoinWithoutAlias()
    {
        String expectSql = "" +
                "SELECT * FROM table_t1 _alias_t1 " +
                "JOIN table_t2 _alias_t2 " +
                "ON _alias_t1.key = _alias_t2.key";

        assertRewrite(
                "SELECT * FROM graphmdl.w1.t1 JOIN graphmdl.w1.t2 ON t1.key = t2.key",
                null,
                null,
                expectSql);
        assertRewrite(
                "SELECT * FROM w1.t1 JOIN w1.t2 ON t1.key = t2.key",
                "graphmdl",
                null,
                expectSql);
        assertRewrite(
                "SELECT * FROM t1 JOIN t2 ON t1.key = t2.key",
                "graphmdl",
                "w1",
                expectSql);
    }

    @Test(dataProvider = "twoTableProvider")
    public void testUnion(TwoTableTestData testData)
    {
        assertTwoTables("SELECT * FROM {0} UNION SELECT * FROM {1}", testData);
    }

    @Test(dataProvider = "oneTableProvider")
    public void testWithQuery(OneTableTestData testData)
    {
        assertOneTable("WITH table_alias AS (SELECT * FROM {0}) SELECT * FROM table_alias", testData);
    }

    @Test(dataProvider = "twoTableProvider")
    public void testWithQueryTwoTable(TwoTableTestData testData)
    {
        assertTwoTables("WITH " +
                "table_alias1 AS (SELECT * FROM {0})," +
                "table_alias2 AS (SELECT * FROM {1}) " +
                "SELECT * FROM table_alias1 JOIN table_alias2 ON table_alias1.key = table_alias2.key", testData);
    }

    @Test(dataProvider = "oneTableProvider")
    public void testSubquery(OneTableTestData testData)
    {
        assertOneTable("SELECT * FROM (SELECT * FROM {0}) AS table_alias", testData);
    }

    @Test(dataProvider = "oneTableProvider")
    public void testInSubquery(OneTableTestData testData)
    {
        assertOneTable("SELECT * FROM {0} WHERE key IN (SELECT key FROM {0})", testData);
    }

    @Test(dataProvider = "oneTableProvider")
    public void testRewriteColumns(OneTableTestData testData)
    {
        assertOneTable("SELECT {0}.column FROM {0}", testData);
    }

    @Test(dataProvider = "oneTableProvider")
    public void testRewriteColumnsCallFunctionInWhere(OneTableTestData testData)
    {
        String sql = "SELECT count(*) AS \"count\" " +
                "FROM {0} " +
                "WHERE date_trunc('day', {0}.column) BETWEEN date_trunc('day', date_add('day', -30, now())) AND date_trunc('day', date_add('day', -1, now()))";
        assertOneTable(sql, testData);
    }

    @Test
    public void testTpchSql1()
    {
        String sql = "SELECT " +
                "returnflag, " +
                "linestatus, " +
                "SUM(quantity) AS sum_qty, " +
                "SUM(extendedprice) AS sum_base_price, " +
                "SUM(extendedprice * (1 - discount)) AS sum_disc_price, " +
                "SUM(extendedprice * (1 - discount) * (1 + tax)) AS sum_charge, " +
                "AVG(quantity) AS avg_qty, " +
                "AVG(extendedprice) AS avg_price, " +
                "AVG(discount) AS avg_disc, " +
                "COUNT(*) AS count_order " +
                "FROM " +
                "{0} " +
                "WHERE " +
                "shipdate <= DATE ''1998-12-01'' - interval ''1'' day " +
                "GROUP BY " +
                "returnflag, " +
                "linestatus " +
                "ORDER BY " +
                "returnflag, " +
                "linestatus";

        assertRewrite(MessageFormat.format(sql, "lineitem"),
                "graphmdl",
                "tpch",
                MessageFormat.format(sql, "table_lineitem"));
    }

    @Test
    public void testTpchSql2()
    {
        String sql = "SELECT " +
                "s.acctbal, " +
                "s.name, " +
                "n.name, " +
                "p.partkey, " +
                "p.mfgr, " +
                "s.address, " +
                "s.phone, " +
                "s.comment " +
                "FROM " +
                "{0} p, " +
                "{1} s, " +
                "{2} ps, " +
                "{3} n, " +
                "{4} r " +
                "WHERE " +
                "p.partkey = ps.partkey " +
                "AND s.suppkey = ps.suppkey " +
                "AND p.size = 1 " +
                "AND p.type LIKE ''%'' " +
                "AND s.nationkey = n.nationkey " +
                "AND n.regionkey = r.regionkey " +
                "AND r.name = ''Asia'' " +
                "AND ps.supplycost = ( " +
                "SELECT " +
                "min(ps.supplycost) AS min_cost " +
                "FROM " +
                "{0} p, " +
                "{1} s, " +
                "{3} n, " +
                "{4} r " +
                "WHERE " +
                "p.partkey = ps.partkey " +
                "AND s.suppkey = ps.suppkey " +
                "AND s.nationkey = n.nationkey " +
                "AND n.regionkey = r.regionkey " +
                "AND r.name = ''Asia'' " +
                ") " +
                "ORDER BY " +
                "s.acctbal DESC, " +
                "n.name, " +
                "s.name, " +
                "p.partkey";

        assertRewrite(MessageFormat.format(sql, "part", "supplier", "partsupp", "nation", "region"),
                "graphmdl",
                "tpch",
                MessageFormat.format(sql,
                        "table_part",
                        "table_supplier",
                        "table_partsupp",
                        "table_nation",
                        "table_region"));
    }

    @Test
    public void testTpchSql3()
    {
        String sql = "SELECT " +
                "l.orderkey, " +
                "SUM(l.extendedprice * (1 - l.discount)) AS revenue, " +
                "o.orderdate, " +
                "o.shippriority " +
                "FROM " +
                "{0} c, " +
                "{1} o, " +
                "{2} l " +
                "WHERE " +
                "c.mktsegment = '''' " +
                "AND c.custkey = o.custkey " +
                "AND l.orderkey = o.orderkey " +
                "AND o.orderdate < DATE ''2000-01-01'' " +
                "AND l.shipdate > DATE ''2000-01-01'' " +
                "GROUP BY " +
                "l.orderkey, " +
                "o.orderdate, " +
                "o.shippriority " +
                "ORDER BY " +
                "revenue DESC, " +
                "o.orderdate";

        assertRewrite(MessageFormat.format(sql, "customer", "orders", "lineitem"),
                "graphmdl",
                "tpch",
                MessageFormat.format(sql,
                        "table_customer",
                        "table_orders",
                        "table_lineitem"));
    }

    @Test
    public void testTpchSql7()
    {
        String sql = "SELECT " +
                "supp_nation, " +
                "cust_nation, " +
                "l_year, " +
                "SUM(volume) AS revenue " +
                "FROM " +
                "( " +
                "SELECT " +
                "n1.n_name AS supp_nation, " +
                "n2.n_name AS cust_nation, " +
                "EXTRACT(YEAR FROM l_shipdate) AS l_year, " +
                "l_extendedprice * (1 - l_discount) AS volume " +
                "FROM " +
                "{0}, " +
                "{1}, " +
                "{2}, " +
                "{3}, " +
                "{4} n1, " +
                "{4} n2 " +
                "WHERE " +
                "s_suppkey = l_suppkey " +
                "AND o_orderkey = l_orderkey " +
                "AND c_custkey = o_custkey " +
                "AND s_nationkey = n1.n_nationkey " +
                "AND c_nationkey = n2.n_nationkey " +
                "AND ( " +
                "(n1.n_name = '''' AND n2.n_name = '''') " +
                "OR (n1.n_name = '''' AND n2.n_name = '''') " +
                ") " +
                "AND l_shipdate BETWEEN DATE ''1995-01-01'' AND DATE ''1996-12-31'' " +
                ") as shipping " +
                "GROUP BY " +
                "supp_nation, " +
                "cust_nation, " +
                "l_year " +
                "ORDER BY " +
                "supp_nation, " +
                "cust_nation, " +
                "l_year";

        assertRewrite(MessageFormat.format(sql, "supplier", "lineitem", "orders", "customer", "nation"),
                "graphmdl",
                "tpch",
                MessageFormat.format(sql,
                        "table_supplier _alias_supplier",
                        "table_lineitem _alias_lineitem",
                        "table_orders _alias_orders",
                        "table_customer _alias_customer",
                        "table_nation"));
    }

    @Test
    public void testTpchSql11()
    {
        String sql = "SELECT " +
                "ps.partkey, " +
                "SUM(ps.supplycost * ps.availqty) AS value " +
                "FROM " +
                "{0} ps, " +
                "{1} s, " +
                "{2} n " +
                "WHERE " +
                "ps.suppkey = s.suppkey " +
                "AND s.nationkey = n.nationkey " +
                "AND n.name = ''UNITED STATES'' " +
                "GROUP BY " +
                "ps.partkey HAVING " +
                "SUM(ps.supplycost * ps.availqty) > ( " +
                "SELECT " +
                "(SUM(ps.supplycost * ps.availqty) * 1) supplycoseqty " +
                "FROM " +
                "{0} ps, " +
                "{1} s, " +
                "{2} n " +
                "WHERE " +
                "ps.suppkey = s.suppkey " +
                "AND s.nationkey = n.nationkey " +
                "AND n.name = ''UNITED STATES'' " +
                ") " +
                "ORDER BY " +
                "value DESC";

        assertRewrite(MessageFormat.format(sql, "partsupp", "supplier", "nation"),
                "graphmdl",
                "tpch",
                MessageFormat.format(sql,
                        "table_partsupp",
                        "table_supplier",
                        "table_nation"));
    }

    @Test
    public void testTpchSql17()
    {
        String sql = "SELECT " +
                "SUM(l_extendedprice) / 7.0 AS avg_yearly " +
                "FROM " +
                "{0}, " +
                "{1} " +
                "WHERE " +
                "p_partkey = l_partkey " +
                "AND p_brand = '''' " +
                "AND p_container = '''' " +
                "AND l_quantity < ( " +
                "SELECT " +
                "(0.2 * AVG(l_quantity)) avgqty " +
                "FROM " +
                "{2} " +
                "WHERE " +
                "l_partkey = p_partkey " +
                ")";

        assertRewrite(MessageFormat.format(sql, "lineitem", "part", "lineitem"),
                "graphmdl",
                "tpch",
                MessageFormat.format(sql,
                        "table_lineitem _alias_lineitem",
                        "table_part _alias_part",
                        "table_lineitem"));
    }

    @Test
    public void testTpchSql20()
    {
        String sql = "SELECT " +
                "s.name, " +
                "s.address " +
                "FROM " +
                "{0} s, " +
                "{1} n " +
                "WHERE " +
                "s.suppkey IN ( " +
                "SELECT " +
                "ps.suppkey " +
                "FROM " +
                "{2} ps " +
                "WHERE " +
                "ps.partkey IN ( " +
                "SELECT " +
                "p.partkey " +
                "FROM " +
                "{3} p " +
                "WHERE " +
                "p.name LIKE ''%'' " +
                ") " +
                "AND ps.availqty > ( " +
                "SELECT " +
                "(0.5 * SUM(l.quantity)) sumqty " +
                "FROM " +
                "{4} l " +
                "WHERE " +
                "l.partkey = ps.partkey " +
                "AND l.suppkey = ps.suppkey " +
                "AND l.shipdate >= DATE ''1995-01-01'' " +
                "AND l.shipdate < DATE ''1995-01-01'' + interval ''1'' year " +
                ") " +
                ") " +
                "AND s.nationkey = n.nationkey " +
                "AND n.name = ''CANADA'' " +
                "ORDER BY " +
                "s.name";

        assertRewrite(MessageFormat.format(sql, "supplier", "nation", "partsupp", "part", "lineitem"),
                "graphmdl",
                "tpch",
                MessageFormat.format(sql,
                        "table_supplier",
                        "table_nation",
                        "table_partsupp",
                        "table_part",
                        "table_lineitem"));
    }

    @Test
    public void testTpchSql21()
    {
        String sql = "SELECT " +
                "s.name, " +
                "COUNT(*) AS numwait " +
                "FROM " +
                "{0} s, " +
                "{1} l1, " +
                "{2} o, " +
                "{3} n " +
                "WHERE " +
                "s.suppkey = l1.suppkey " +
                "AND o.orderkey = l1.orderkey " +
                "AND o.orderstatus = ''F'' " +
                "AND l1.receiptdate > l1.commitdate " +
                "AND EXISTS ( " +
                "SELECT " +
                "* " +
                "FROM " +
                "{1} l2 " +
                "WHERE " +
                "l2.orderkey = l1.orderkey " +
                "and l2.suppkey <> l1.suppkey " +
                ") " +
                "AND NOT EXISTS ( " +
                "SELECT " +
                "* " +
                "FROM " +
                "{1} l3 " +
                "WHERE " +
                "l3.orderkey = l1.orderkey " +
                "AND l3.suppkey <> l1.suppkey " +
                "AND l3.receiptdate > l3.commitdate " +
                ") " +
                "AND s.nationkey = n.nationkey " +
                "AND n.name = ''CANADA'' " +
                "GROUP BY " +
                "s.name " +
                "ORDER BY " +
                "numwait DESC, " +
                "s.name";

        assertRewrite(MessageFormat.format(sql, "supplier", "lineitem", "orders", "nation"),
                "graphmdl",
                "tpch",
                MessageFormat.format(sql,
                        "table_supplier",
                        "table_lineitem",
                        "table_orders",
                        "table_nation",
                        "table_lineitem",
                        "table_lineitem"));
    }

    @Test
    public void testEscapeDash()
    {
        assertRewrite(
                "SELECT * FROM \"t-1\"",
                "graphmdl",
                "escape",
                "SELECT * FROM \"table_t-1\"");
    }

    @DataProvider(name = "aliasSameNameProvider")
    public Object[][] aliasSameNameProvider()
    {
        return new Object[][] {
                {"SELECT lineitem.column column FROM {0} lineitem"},
                {"SELECT lineitem.column AS column FROM {0} lineitem"},
                {"SELECT lineitem.column AS column FROM {0} AS lineitem"},
                {"SELECT \"lineitem\".\"column\" AS \"column\" FROM {0} AS \"lineitem\""},
        };
    }

    @Test(dataProvider = "aliasSameNameProvider")
    public void testAliasSameName(String sql)
    {
        assertRewrite(MessageFormat.format(sql, "tpch.lineitem"),
                "graphmdl",
                "tpch",
                MessageFormat.format(sql,
                        "table_lineitem"));
    }

    @DataProvider(name = "columnDereferenceProvider")
    public Object[][] columnDereferenceProvider()
    {
        return new Object[][] {
                {"SELECT lineitem.orderkey FROM lineitem"},
                {"SELECT lineitem.orderkey FROM tpch.lineitem"},
                {"SELECT tpch.lineitem.orderkey FROM tpch.lineitem"},
                {"SELECT lineitem.orderkey FROM graphmdl.tpch.lineitem"},
                {"SELECT tpch.lineitem.orderkey FROM graphmdl.tpch.lineitem"},
                {"SELECT graphmdl.tpch.lineitem.orderkey FROM graphmdl.tpch.lineitem"},
        };
    }

    @Test(dataProvider = "columnDereferenceProvider")
    public void testColumnDereferenceRewrite(String sql)
    {
        assertRewrite(
                sql,
                "graphmdl",
                "tpch",
                MessageFormat.format("SELECT {0}.orderkey FROM {0}", "table_lineitem"));
    }

    @Test
    public void testUnexpectedStatement()
    {
        // todo: should throw an exception; only accept select and with statement?
    }

    private void assertOneTable(String sqlFormat, OneTableTestData testData)
    {
        assertRewrite(MessageFormat.format(sqlFormat, testData.table),
                testData.defaultCatalog,
                testData.defaultSchema,
                MessageFormat.format(sqlFormat, "table_t1"));
    }

    private void assertTwoTables(String sqlFormat, TwoTableTestData testData)
    {
        assertRewrite(MessageFormat.format(sqlFormat, testData.table1, testData.table2),
                testData.defaultCatalog,
                testData.defaultSchema,
                MessageFormat.format(sqlFormat, "table_t1", "table_t2"));

        assertRewrite(MessageFormat.format(sqlFormat, testData.table2, testData.table1),
                testData.defaultCatalog,
                testData.defaultSchema,
                MessageFormat.format(sqlFormat, "table_t2", "table_t1"));
    }

    private void assertRewrite(
            String sql,
            String defaultCatalog,
            String defaultSchema,
            String expectSql)
    {
        assertRewrite(
                sql,
                defaultCatalog,
                defaultSchema,
                expectSql,
                this::toPreAggregationTable);
    }

    private void assertRewrite(
            String sql,
            String defaultCatalog,
            String defaultSchema,
            String expectSql,
            Function<CatalogSchemaTableName, Optional<String>> tableConverter)
    {
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog(defaultCatalog)
                .setSchema(defaultSchema)
                .build();

        Statement result = PreAggregationRewrite.rewrite(
                sessionContext,
                sqlParser,
                sql,
                tableConverter);

        Statement expect = sqlParser.createStatement(expectSql, new ParsingOptions(AS_DECIMAL));
        assertThat(formatSql(result)).isEqualTo(formatSql(expect));
        assertThat(result).isEqualTo(expect);
    }

    private static class OneTableTestData
    {
        private final String defaultCatalog;
        private final String defaultSchema;
        private final String table;

        private static OneTableTestData create(String defaultCatalog, String defaultSchema, String table)
        {
            return new OneTableTestData(defaultCatalog, defaultSchema, table);
        }

        private OneTableTestData(String defaultCatalog, String defaultSchema, String table)
        {
            this.defaultCatalog = defaultCatalog;
            this.defaultSchema = defaultSchema;
            this.table = table;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("table", format("%s.%s.%s", defaultCatalog, defaultSchema, table))
                    .toString();
        }
    }

    private static class TwoTableTestData
    {
        private final String defaultCatalog;
        private final String defaultSchema;
        private final String table1;
        private final String table2;

        private static TwoTableTestData create(String defaultCatalog, String defaultSchema, String table1, String table2)
        {
            return new TwoTableTestData(defaultCatalog, defaultSchema, table1, table2);
        }

        private TwoTableTestData(String defaultCatalog, String defaultSchema, String table1, String table2)
        {
            this.defaultCatalog = defaultCatalog;
            this.defaultSchema = defaultSchema;
            this.table1 = table1;
            this.table2 = table2;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("table1", format("%s.%s.%s", defaultCatalog, defaultSchema, table1))
                    .add("table2", format("%s.%s.%s", defaultCatalog, defaultSchema, table2))
                    .toString();
        }
    }

    private Optional<String> toPreAggregationTable(CatalogSchemaTableName tableName)
    {
        return Optional.ofNullable(PRE_AGG.get(tableName));
    }
}
