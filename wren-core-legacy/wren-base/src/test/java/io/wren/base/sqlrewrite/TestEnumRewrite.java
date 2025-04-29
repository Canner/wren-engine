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

package io.wren.base.sqlrewrite;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import io.wren.base.AnalyzedMDL;
import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.EnumDefinition;
import io.wren.base.dto.EnumValue;
import io.wren.base.dto.Model;
import org.testng.annotations.Test;

import java.util.List;

import static io.wren.base.sqlrewrite.EnumRewrite.ENUM_REWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestEnumRewrite
        extends AbstractTestFramework
{
    private final WrenMDL wrenMDL;

    private static final SqlParser SQL_PARSER = new SqlParser();

    public TestEnumRewrite()
    {
        this.wrenMDL = WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("People",
                                "select * from (values (1, 'user1', 'MALE', 'tw'), (2, 'user2', 'FEMALE', 'jp'), (3, 'user3', 'MALE', 'us')) People(userId, name)",
                                List.of(
                                        Column.column("userId", WrenTypes.INTEGER, null, true),
                                        Column.column("name", WrenTypes.VARCHAR, null, true),
                                        Column.column("sex", "Sex", null, true),
                                        Column.column("country", "Country", null, true)),
                                "userId")))
                .setEnumDefinitions(List.of(
                        EnumDefinition.enumDefinition("Sex", List.of(EnumValue.enumValue("MALE"), EnumValue.enumValue("FEMALE"))),
                        EnumDefinition.enumDefinition("Country", List.of(EnumValue.enumValue("TAIWAN", "tw"), EnumValue.enumValue("JAPAN", "jp"), EnumValue.enumValue("USA", "us")))))
                .build());
    }

    @Test
    public void testBasic()
    {
        assertThat(rewrite("select Sex.MALE")).isEqualTo(parse("select 'MALE'"));
        assertThat(rewrite("select Country.TAIWAN")).isEqualTo(parse("select 'tw'"));
        assertThat(rewrite("select sex = Sex.MALE from People")).isEqualTo(rewrite("select sex = 'MALE' from People"));
        assertThat(rewrite("select country = Country.JAPAN from People")).isEqualTo(rewrite("select country = 'jp' from People"));
        assertThat(rewrite("select * from People WHERE sex = Sex.MALE")).isEqualTo(rewrite("select * from People WHERE sex = 'MALE'"));
    }

    @Test
    public void testNoRewrite()
    {
        assertNoRewrite("select MALE");
        assertNoRewrite("select country.TAIWAN");
    }

    @Test
    public void testInvalidEnum()
    {
        assertThatThrownBy(() -> rewrite("select Country.China"))
                .hasMessage("Enum value 'China' not found in enum 'Country'");
        assertThatThrownBy(() -> rewrite("select Country.taiwan"))
                .hasMessage("Enum value 'taiwan' not found in enum 'Country'");
        assertThatThrownBy(() -> rewrite("select Country.tw"))
                .hasMessage("Enum value 'tw' not found in enum 'Country'");
    }

    private void assertNoRewrite(String sql)
    {
        assertThat(rewrite(sql)).isEqualTo(parse(sql));
    }

    private Statement rewrite(String sql)
    {
        return ENUM_REWRITE.apply(parse(sql), DEFAULT_SESSION_CONTEXT, new AnalyzedMDL(wrenMDL, null));
    }

    private Statement parse(String sql)
    {
        return SQL_PARSER.createStatement(sql, new ParsingOptions());
    }
}
