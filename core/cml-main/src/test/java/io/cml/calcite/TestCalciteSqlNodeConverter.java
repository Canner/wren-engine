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

import io.cml.TestingMetadata;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCalciteSqlNodeConverter
{
    @DataProvider
    public Object[][] normalQuery()
    {
        return new Object[][] {
                {"testRelationWithPrefix", "select orderkey, custkey from tpch.tiny.orders join tpch.tiny.lineitem on tpch.tiny.orders.orderkey = tpch.tiny.lineitem.orderkey"},
                {"testRowNode", "select * from (values ('rows1', 10), ('rows2', 20)) as t(col1, col2) where col2 = ?"},
                {"testUnion", "select * from (select 1, 'foo' union select 2, 'bar' union select 3, 'test') as t(col1, col2)"},
                {"testUnionAll", "select * from (select 1, 'foo' union all select 2, 'bar' union all select 3, 'test') as t(col1, col2)"},
                {"testCastAs", "SELECT CAST(true AS boolean) col_0, CAST(false AS boolean) col_1, CAST(123456789012 AS bigint) col_2, CAST(32456 AS smallint) col_4, " +
                        "CAST(125 AS tinyint) col_5, CAST(123.45 AS double) col_6, CAST(123.45 AS real) col_7"},
                {"testBinaryLiteral", "SELECT X'68656C6C6F' col_0"},
                {"testQuotedDereferenceExpression", "select \"canner-1234\".\"schema-1234\".\"table-1234\".\"col1\" from \"canner-1234\".\"schema-1234\".\"table-1234\""},
                {"testSubscript", "select generate_array(1, 10)[1]"},
                {"testUnnset", "select * from unnest (generate_array(1, 10))"},
                {"testJdbcTypeInfoQuery", "SELECT\n" +
                        "  (typinput = 5004152699888335811) is_array\n" +
                        ", typtype\n" +
                        ", typname\n" +
                        ", \"cannerflow-286003\".pg_catalog.pg_type.oid\n" +
                        "FROM\n" +
                        "  \"cannerflow-286003\".pg_catalog.pg_type\n" +
                        "LEFT JOIN (\n" +
                        "   SELECT\n" +
                        "     ns.oid nspoid\n" +
                        "   , nspname\n" +
                        "   , r.r\n" +
                        "   FROM\n" +
                        "     \"cannerflow-286003\".pg_catalog.pg_namespace ns\n" +
                        "   INNER JOIN (\n" +
                        "      SELECT\n" +
                        "        s.r\n" +
                        "      , current_schemas(false)[s.r] nspname\n" +
                        "      FROM\n" +
                        "        UNNEST(generate_array(1, array_upper(current_schemas(false), 1)))  s (r)\n" +
                        "   )  r USING (nspname)\n" +
                        ")  sp ON (sp.nspoid = typnamespace)"},
                {"arrayConstructor", "select array[1, 2, 3], array['a', 'b', 'c']"},
                {"testNullIf", "SELECT nullif(a.attidentity, '') FROM \"cannerflow-286003\".pg_catalog.pg_attribute a"},
                {"testRowNumber", "SELECT row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum ASC) attnum\n" +
                        "FROM \"cannerflow-286003\".pg_catalog.pg_attribute a"}
        };
    }

    @Test(dataProvider = "normalQuery")
    public void testNormalQuery(@SuppressWarnings("unused") String name, String sql)
            throws SqlParseException
    {
        SqlParser sqlParser = new SqlParser();
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        SqlNode processedNode = CalciteSqlNodeConverter.convert(statement, new Analysis(), new TestingMetadata());

        org.apache.calcite.sql.parser.SqlParser parser = org.apache.calcite.sql.parser.SqlParser.create(sql);
        SqlNode calciteNode = parser.parseQuery();

        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter();
        String trinoSql = sqlPrettyWriter.format(processedNode);

        sqlPrettyWriter.reset();
        String calciteSql = sqlPrettyWriter.format(calciteNode);
        assertThat(trinoSql).isEqualToIgnoringCase(calciteSql);
    }

    @DataProvider
    public Object[][] tpchQuery()
    {
        return new Object[][] {
                {"tpch/1.sql"},
                {"tpch/2.sql"},
                {"tpch/3.sql"},
                {"tpch/4.sql"},
                {"tpch/5.sql"},
                {"tpch/6.sql"},
                {"tpch/7.sql"},
                {"tpch/8.sql"},
                {"tpch/9.sql"},
                {"tpch/10.sql"},
                {"tpch/11.sql"},
                {"tpch/12.sql"},
                {"tpch/13.sql"},
                {"tpch/14.sql"},
                {"tpch/15.sql"},
                {"tpch/16.sql"},
                {"tpch/17.sql"},
                {"tpch/18.sql"},
                {"tpch/19.sql"},
                {"tpch/20.sql"},
                {"tpch/21.sql"},
                {"tpch/22.sql"},
        };
    }

    @Test(dataProvider = "tpchQuery")
    public void testTpch(String sqlFile)
            throws SqlParseException, IOException, URISyntaxException
    {
        SqlParser sqlParser = new SqlParser();

        Path path = Paths.get(requireNonNull(getClass().getClassLoader().getResource(sqlFile)).toURI());
        String sql = Files.readString(path);

        Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        SqlNode processedNode = CalciteSqlNodeConverter.convert(statement, new Analysis(), new TestingMetadata());

        org.apache.calcite.sql.parser.SqlParser parser = org.apache.calcite.sql.parser.SqlParser.create(sql);
        SqlNode calciteNode = parser.parseQuery();

        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter();
        String trinoSql = sqlPrettyWriter.format(processedNode);

        sqlPrettyWriter.reset();
        String calciteSql = sqlPrettyWriter.format(calciteNode);
        assertThat(trinoSql).isEqualToIgnoringCase(calciteSql);
    }

    @DataProvider
    public Object[][] forCaseSensitive()
    {
        return new Object[][] {
                {"SELECT clerk FROM tpch.tiny.orders", "SELECT \"clerk\" FROM \"tpch\".\"tiny\".\"orders\""},
                {"SELECT CLERK FROM tpch.tiny.orders", "SELECT \"CLERK\" FROM \"tpch\".\"tiny\".\"orders\""},
                {"SELECT \"CLERK\" FROM \"tpch\".\"tiny\".\"ORDERS\"", "SELECT \"CLERK\" FROM \"tpch\".\"tiny\".\"ORDERS\""},
                {"SELECT clerk FROM \"tpch\".\"tiny\".\"ORDERS\"", "SELECT \"clerk\" FROM \"tpch\".\"tiny\".\"ORDERS\""}
        };
    }

    @Test(dataProvider = "forCaseSensitive")
    public void testCaseSensitive(String before, String after)
    {
        SqlParser sqlParser = new SqlParser();
        Statement statement = sqlParser.createStatement(before, new ParsingOptions());
        SqlNode processedNode = CalciteSqlNodeConverter.convert(statement, new Analysis(), new TestingMetadata());
        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter(
                SqlWriterConfig.of().withClauseStartsLine(false).withDialect(CalciteSqlDialect.DEFAULT));
        String processed = sqlPrettyWriter.format(processedNode);

        assertThat(processed).isEqualTo(after);
    }
}
