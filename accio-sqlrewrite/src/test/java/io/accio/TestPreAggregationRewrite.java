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

import com.google.common.collect.ImmutableMap;
import io.accio.base.AccioMDL;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.SessionContext;
import io.accio.sqlrewrite.PreAggregationRewrite;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.accio.base.AccioTypes.DATE;
import static io.accio.base.AccioTypes.DECIMAL;
import static io.accio.base.AccioTypes.INTEGER;
import static io.accio.base.AccioTypes.TIMESTAMP;
import static io.accio.base.AccioTypes.VARCHAR;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Metric.metric;
import static io.accio.base.dto.Model.model;
import static io.accio.base.dto.TimeGrain.timeGrain;
import static io.accio.base.dto.TimeUnit.YEAR;
import static io.accio.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static io.trino.sql.SqlFormatter.Dialect.DUCKDB;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPreAggregationRewrite
{
    private SqlParser sqlParser;
    private AccioMDL accioMDL;

    private static final Map<CatalogSchemaTableName, String> METRIC_PREAGG_NAME_MAPPING =
            ImmutableMap.<CatalogSchemaTableName, String>builder()
                    .put(new CatalogSchemaTableName("accio", "test", "Collection"), "table_Collection")
                    .put(new CatalogSchemaTableName("accio", "test", "AvgCollection"), "table_AvgCollection")
                    .put(new CatalogSchemaTableName("accio", "test", "t-1"), "table_t-1")
                    .put(new CatalogSchemaTableName("accio", "test", "Album"), "table_Album")
                    .put(new CatalogSchemaTableName("accio", "test", "Tag"), "table_Tag")
                    .build();

    @BeforeClass
    public void init()
    {
        sqlParser = new SqlParser();
        accioMDL = AccioMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        model("Album",
                                "select * from (values (1, 'Gusare', 'ZUTOMAYO', 2560, DATE '2023-03-29', TIMESTAMP '2023-04-27 06:06:06'), " +
                                        "(2, 'HisoHiso Banashi', 'ZUTOMAYO', 1500, DATE '2023-04-29', TIMESTAMP '2023-05-27 07:07:07'), " +
                                        "(3, 'Dakara boku wa ongaku o yameta', 'Yorushika', 2553, DATE '2023-05-29', TIMESTAMP '2023-06-27 08:08:08')) " +
                                        "album(id, name, author, price, publish_date, release_date)",
                                List.of(
                                        column("id", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        column("author", VARCHAR, null, true),
                                        column("price", INTEGER, null, true),
                                        column("publish_date", DATE, null, true),
                                        column("release_date", TIMESTAMP, null, true)),
                                true),
                        model("Tag",
                                "select * from (VALUES\n" +
                                        " (1, 'U2',     5),\n" +
                                        " (2, 'Blur',   5),\n" +
                                        " (3, 'Oasis',  5),\n" +
                                        " (4, '2Pac',   6),\n" +
                                        " (5, 'Rock',   7),\n" +
                                        " (6, 'Rap',    7),\n" +
                                        " (7, 'Music',  9),\n" +
                                        " (8, 'Movies', 9),\n" +
                                        " (9, 'Art', NULL))\n" +
                                        "tag(id, name, subclassof)",
                                List.of(
                                        column("id", INTEGER, null, true),
                                        column("name", VARCHAR, null, true),
                                        column("subclassof", INTEGER, null, false)),
                                true)))
                .setMetrics(List.of(
                        metric(
                                "Collection",
                                "Album",
                                List.of(
                                        column("author", VARCHAR, null, true),
                                        column("album_name", VARCHAR, null, true, "Album.name")),
                                List.of(column("price", INTEGER, null, true, "sum(Album.price)")),
                                List.of(
                                        timeGrain("p_date", "Album.publish_date", List.of(YEAR)),
                                        timeGrain("r_date", "Album.release_date", List.of(YEAR))),
                                true),
                        metric(
                                "AvgCollection",
                                "Album",
                                List.of(
                                        column("author", VARCHAR, null, true),
                                        column("album_name", VARCHAR, null, true, "Album.name")),
                                List.of(column("price", DECIMAL, null, true, "avg(Album.price)")),
                                List.of(
                                        timeGrain("p_date", "Album.publish_date", List.of(YEAR)),
                                        timeGrain("r_date", "Album.release_date", List.of(YEAR))),
                                true),
                        metric(
                                "t-1",
                                "Album",
                                List.of(
                                        column("author", VARCHAR, null, true),
                                        column("album_name", VARCHAR, null, true, "Album.name")),
                                List.of(column("price", INTEGER, null, true, "avg(Album.price)")),
                                List.of(
                                        timeGrain("p_date", "Album.publish_date", List.of(YEAR)),
                                        timeGrain("r_date", "Album.release_date", List.of(YEAR))),
                                true)))
                .build());
    }

    @DataProvider(name = "oneTableProvider")
    public Object[][] oneTableProvider()
    {
        return new Object[][] {
                {OneTableTestData.create("accio", "test", "accio.test.Collection")},
                {OneTableTestData.create("accio", "test", "test.Collection")},
                {OneTableTestData.create("accio", "test", "Collection")},
                {OneTableTestData.create("accio", "w2", "accio.test.Collection")},
                {OneTableTestData.create("accio", "w2", "test.Collection")},
                {OneTableTestData.create("other", "test", "accio.test.Collection")},
                {OneTableTestData.create("other", "w2", "accio.test.Collection")},
        };
    }

    @DataProvider(name = "twoTableProvider")
    public Object[][] twoTableProvider()
    {
        return new Object[][] {
                {TwoTableTestData.create("accio", "test", "accio.test.Collection", "accio.test.AvgCollection")},
                {TwoTableTestData.create("accio", "test", "accio.test.Collection", "test.AvgCollection")},
                {TwoTableTestData.create("accio", "test", "test.Collection", "test.AvgCollection")},
                {TwoTableTestData.create("accio", "test", "Collection", "AvgCollection")},
        };
    }

    @Test(dataProvider = "oneTableProvider")
    public void testSelect(OneTableTestData testData)
    {
        assertOneTable("SELECT * FROM {0}", testData);
    }

    @Test
    public void testSelectModel()
    {
        assertRewrite("select * from Album",
                "accio",
                "test",
                "select * from table_Album");
    }

    @Test
    public void testSelectWithRecursive()
    {
        // sample from duckdb https://duckdb.org/docs/sql/query_syntax/with.html
        assertRewrite("WITH RECURSIVE tag_hierarchy(id, source, path) AS (\n" +
                        "  SELECT id, name, name AS path\n" +
                        "  FROM Tag\n" +
                        "  WHERE subclassof IS NULL\n" +
                        "UNION ALL\n" +
                        "  SELECT Tag.id, Tag.name, CONCAT(Tag.name, ',', tag_hierarchy.path)\n" +
                        "  FROM Tag, tag_hierarchy\n" +
                        "  WHERE Tag.subclassof = tag_hierarchy.id\n" +
                        ")\n" +
                        "SELECT path\n" +
                        "FROM tag_hierarchy\n" +
                        "WHERE source = 'Oasis'",
                "accio",
                "test",
                "WITH RECURSIVE tag_hierarchy(id, source, path) AS (\n" +
                        "  SELECT id, name, name AS path\n" +
                        "  FROM table_Tag\n" +
                        "  WHERE subclassof IS NULL\n" +
                        "UNION ALL\n" +
                        "  SELECT table_Tag.id, table_Tag.name, CONCAT(table_Tag.name, ',', tag_hierarchy.path)\n" +
                        "  FROM table_Tag, tag_hierarchy\n" +
                        "  WHERE table_Tag.subclassof = tag_hierarchy.id\n" +
                        ")\n" +
                        "SELECT path\n" +
                        "FROM tag_hierarchy\n" +
                        "WHERE source = 'Oasis'");
    }

    @Test(dataProvider = "twoTableProvider")
    public void testJoin(TwoTableTestData testData)
    {
        assertTwoTables("SELECT * FROM {0} a LEFT JOIN {1} b ON a.author = b.author", testData);
    }

    @Test
    public void testJoinWithoutAlias()
    {
        String expectSql = "" +
                "SELECT * FROM table_Collection " +
                "JOIN table_AvgCollection " +
                "ON table_Collection.author = table_AvgCollection.author";

        assertRewrite(
                "SELECT * FROM accio.test.Collection JOIN accio.test.AvgCollection ON Collection.author = AvgCollection.author",
                "accio",
                "test",
                expectSql);
        assertRewrite(
                "SELECT * FROM test.Collection JOIN test.AvgCollection ON Collection.author = AvgCollection.author",
                "accio",
                "test",
                expectSql);
        assertRewrite(
                "SELECT * FROM Collection JOIN AvgCollection ON Collection.author = AvgCollection.author",
                "accio",
                "test",
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
                "SELECT * FROM table_alias1 JOIN table_alias2 ON table_alias1.author = table_alias2.author", testData);
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
        assertOneTable("SELECT {0}.author FROM {0}", testData);
    }

    @Test(dataProvider = "oneTableProvider")
    public void testRewriteColumnsCallFunctionInWhere(OneTableTestData testData)
    {
        String sql = "SELECT count(*) AS \"count\" " +
                "FROM {0} " +
                "WHERE date_trunc('day', {0}.author) BETWEEN date_trunc('day', date_add('day', -30, now())) AND date_trunc('day', date_add('day', -1, now()))";
        assertOneTable(sql, testData);
    }

    @Test
    public void testEscapeDash()
    {
        assertRewrite(
                "SELECT * FROM \"t-1\"",
                "accio",
                "test",
                "SELECT * FROM \"table_t-1\"");
    }

    @DataProvider(name = "aliasSameNameProvider")
    public Object[][] aliasSameNameProvider()
    {
        return new Object[][] {
                {"SELECT Collection.author author FROM {0} Collection"},
                {"SELECT Collection.column AS author FROM {0} Collection"},
                {"SELECT Collection.column AS author FROM {0} AS Collection"},
                {"SELECT \"Collection\".\"author\" AS \"author\" FROM {0} AS \"Collection\""},
        };
    }

    @Test(dataProvider = "aliasSameNameProvider")
    public void testAliasSameName(String sql)
    {
        assertRewrite(MessageFormat.format(sql, "test.Collection"),
                "accio",
                "test",
                MessageFormat.format(sql,
                        "table_Collection"));
    }

    @DataProvider(name = "columnDereferenceProvider")
    public Object[][] columnDereferenceProvider()
    {
        return new Object[][] {
                {"SELECT Collection.author FROM Collection"},
                {"SELECT Collection.author FROM test.Collection"},
                {"SELECT test.Collection.author FROM test.Collection"},
                {"SELECT Collection.author FROM accio.test.Collection"},
                {"SELECT test.Collection.author FROM accio.test.Collection"},
                {"SELECT accio.test.Collection.author FROM accio.test.Collection"},
        };
    }

    @Test(dataProvider = "columnDereferenceProvider")
    public void testColumnDereferenceRewrite(String sql)
    {
        assertRewrite(
                sql,
                "accio",
                "test",
                MessageFormat.format("SELECT {0}.author FROM {0}", "table_Collection"));
    }

    @Test(dataProvider = "oneTableProvider")
    public void testFunction(OneTableTestData testData)
    {
        assertOneTable("SELECT author, count(*) FROM {0} GROUP BY author", testData);
    }

    @Test
    public void testTableAliasScope()
    {
        assertRewrite(
                "with test_a as (SELECT * FROM Collection Collection) select * from Collection",
                "accio",
                "test",
                "with test_a as (SELECT * FROM table_Collection Collection) select * from table_Collection");

        assertRewrite(
                "with test_a as (with AvgCollection as (select * from Collection) select * from AvgCollection) select * from AvgCollection",
                "accio",
                "test",
                "with test_a as (with AvgCollection as (select * from table_Collection) select * from AvgCollection) select * from table_AvgCollection");
    }

    @Test
    public void testDecimalRewrite()
    {
        assertRewrite(
                "SELECT * from AvgCollection where avg = DECIMAL '1.0'",
                "accio",
                "test",
                "SELECT * FROM table_AvgCollection WHERE avg = 1.0");
    }

    @DataProvider(name = "unexpectedStatementProvider")
    public Object[][] unexpectedStatementProvider()
    {
        return new Object[][] {
                {"explain analyze select * from Collection"},
                {"prepare aa from select * from Collection"},
                {"execute aa"},
                {"deallocate prepare aa"},
                {"describe output aa"},
                {"describe input aa"},
                {"explain select * from Collection"},
                {"show tables from test"},
                {"show schemas from accio"},
                {"show catalogs"},
                {"show columns from Collection"},
                {"show stats for Collection"},
                {"show create table Collection"},
                {"show functions"},
                {"show session"},
                {"use accio.test"},
                {"use accio.test"},
                {"set session catalog.name = accio"},
                {"reset session optimize_hash_generation"},
                {"create view test_view as select * from Collection"},
                {"drop view if exists test_view"},
                {"insert into cities values (1, 'San Francisco')"},
                {"call test(name => 'apple', id => 123)"},
                {"delete from lineitem where shipmode = 'AIR'"},
                {"start transaction"},
                {"create role admin"},
                {"drop role admin"},
                {"grant bar to user foo"},
                {"revoke insert, select on orders from alice"},
                {"show grants"},
                {"show role grants from accio"},
                {"commit"},
                {"rollback"},
                {"select 1"},
        };
    }

    @Test(dataProvider = "unexpectedStatementProvider")
    public void testUnexpectedStatement(String sql)
    {
        assertThat(rewritePreAgg(sql)).isEmpty();
    }

    private void assertOneTable(String sqlFormat, OneTableTestData testData)
    {
        assertRewrite(MessageFormat.format(sqlFormat, testData.table),
                testData.defaultCatalog,
                testData.defaultSchema,
                MessageFormat.format(sqlFormat, "table_Collection"));
    }

    private void assertTwoTables(String sqlFormat, TwoTableTestData testData)
    {
        assertRewrite(MessageFormat.format(sqlFormat, testData.table1, testData.table2),
                testData.defaultCatalog,
                testData.defaultSchema,
                MessageFormat.format(sqlFormat, "table_Collection", "table_AvgCollection"));

        assertRewrite(MessageFormat.format(sqlFormat, testData.table2, testData.table1),
                testData.defaultCatalog,
                testData.defaultSchema,
                MessageFormat.format(sqlFormat, "table_AvgCollection", "table_Collection"));
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
        String result = rewritePreAgg(
                sql,
                defaultCatalog,
                defaultSchema,
                tableConverter).orElseThrow(() -> new AssertionError("No rewrite result"));

        Statement expect = sqlParser.createStatement(expectSql, new ParsingOptions(AS_DECIMAL));
        Statement actualStatement = sqlParser.createStatement(result, new ParsingOptions(AS_DECIMAL));
        assertThat(result).isEqualTo(formatSql(expect, DUCKDB));
        assertThat(actualStatement).isEqualTo(expect);
    }

    private Optional<String> rewritePreAgg(String sql)
    {
        return rewritePreAgg(
                sql,
                "accio",
                "test",
                this::toPreAggregationTable);
    }

    private Optional<String> rewritePreAgg(
            String sql,
            String defaultCatalog,
            String defaultSchema,
            Function<CatalogSchemaTableName, Optional<String>> tableConverter)
    {
        SessionContext sessionContext = SessionContext.builder()
                .setCatalog(defaultCatalog)
                .setSchema(defaultSchema)
                .build();
        return PreAggregationRewrite.rewrite(
                sessionContext,
                sql,
                tableConverter,
                accioMDL);
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
        return Optional.ofNullable(METRIC_PREAGG_NAME_MAPPING.get(tableName));
    }
}
