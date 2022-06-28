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

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
        };
    }

    @Test(dataProvider = "normalQuery")
    public void testNormalQuery(String name, String sql)
            throws SqlParseException
    {
        SqlParser sqlParser = new SqlParser();
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions());
        SqlNode processedNode = CalciteSqlNodeConverter.convert(statement, new Analysis());

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

        Statement statement = sqlParser.createStatement(sql, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL));
        SqlNode processedNode = CalciteSqlNodeConverter.convert(statement, new Analysis());

        org.apache.calcite.sql.parser.SqlParser parser = org.apache.calcite.sql.parser.SqlParser.create(sql);
        SqlNode calciteNode = parser.parseQuery();

        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter();
        String trinoSql = sqlPrettyWriter.format(processedNode);

        sqlPrettyWriter.reset();
        String calciteSql = sqlPrettyWriter.format(calciteNode);
        assertThat(trinoSql).isEqualToIgnoringCase(calciteSql);
    }
}
