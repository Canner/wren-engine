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

package io.graphmdl.sqlrewrite;

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.GraphMDLTypes;
import io.graphmdl.testing.AbstractTestFramework;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.testng.annotations.Test;

import java.util.List;

import static io.graphmdl.base.dto.Column.column;
import static io.graphmdl.base.dto.EnumDefinition.enumDefinition;
import static io.graphmdl.base.dto.EnumValue.enumValue;
import static io.graphmdl.base.dto.Model.model;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestEnumRewrite
        extends AbstractTestFramework
{
    private final GraphMDL graphMDL;

    private static final SqlParser SQL_PARSER = new SqlParser();

    public TestEnumRewrite()
    {
        this.graphMDL = GraphMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        model("People",
                                "select * from (values (1, 'user1', 'MALE', 'tw'), (2, 'user2', 'FEMALE', 'jp'), (3, 'user3', 'MALE', 'us')) People(userId, name)",
                                List.of(
                                        column("userId", GraphMDLTypes.INTEGER, null, true),
                                        column("name", GraphMDLTypes.VARCHAR, null, true),
                                        column("sex", "Sex", null, true),
                                        column("country", "Country", null, true)),
                                "userId")))
                .setEnumDefinitions(List.of(
                        enumDefinition("Sex", List.of(enumValue("MALE"), enumValue("FEMALE"))),
                        enumDefinition("Country", List.of(enumValue("TAIWAN", "tw"), enumValue("JAPAN", "jp"), enumValue("USA", "us")))))
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
        return GraphMDLSqlRewrite.GRAPHMDL_SQL_REWRITE.apply(parse(sql), DEFAULT_SESSION_CONTEXT, graphMDL);
    }

    private Statement parse(String sql)
    {
        return SQL_PARSER.createStatement(sql, new ParsingOptions());
    }
}
