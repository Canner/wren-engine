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
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCalciteSqlNodeConverter
{
    private static final ParsingOptions PARSE_AS_DECIMAL = new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL);

    @Test
    public void testConvert()
    {
        SqlParser trinoParser = new SqlParser();
        Statement statement = trinoParser.createStatement("SELECT * FROM test_table", PARSE_AS_DECIMAL);
        SqlNode sqlNode = CalciteSqlNodeConverter.convert(statement);

        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter();
        assertThat(sqlPrettyWriter.format(sqlNode)).isEqualTo("SELECT (*)\n" +
                "FROM \"test_table\"");
    }
}
