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

package io.graphmdl.sqlrewrite.analyzer;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.graphmdl.sqlrewrite.analyzer.ExpressionAnalyzer.DereferenceName.dereferenceName;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestExpressionAnalyzer
{
    public static final SqlParser SQL_PARSER = new SqlParser();

    @DataProvider
    public Object[][] toDereferenceNames()
    {
        return new Object[][] {
                {"author", List.of(dereferenceName("author"))},
                {"author.books[1].name", List.of(dereferenceName("name"), dereferenceName("books", 1), dereferenceName("author"))},
                {"books[1].author.books[1].name", List.of(dereferenceName("name"), dereferenceName("books", 1), dereferenceName("author"), dereferenceName("books", 1))},
                {"author.books[1].shop.owner.name",
                        List.of(dereferenceName("name"), dereferenceName("owner"), dereferenceName("shop"), dereferenceName("books", 1), dereferenceName("author"))}
        };
    }

    @Test(dataProvider = "toDereferenceNames")
    public void testToDereferenceNames(String sql, List<ExpressionAnalyzer.DereferenceName> expected)
    {
        Statement statement = SQL_PARSER.createStatement(String.format("SELECT %s", sql), new ParsingOptions());
        assertThat(ExpressionAnalyzer.toDereferenceNames(getSelectItem(statement))).isEqualTo(expected);
    }

    private static Expression getSelectItem(Statement statement)
    {
        return ((SingleColumn) ((QuerySpecification) ((Query) statement).getQueryBody()).getSelect().getSelectItems().get(0)).getExpression();
    }
}
