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

package io.accio.sqlrewrite;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestLambdaExpressionBodyRewrite
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @DataProvider
    public Object[][] lambdaExpression()
    {
        return new Object[][] {
                {"book.f1.f2.f3", "t.f1.f2.f3"},
                {"book", "'Relationship<Book>'"},
                {"book.f1.a1[1].f2", "t.f1.a1[1].f2"},
                {"concat(book.name, '_1')", "concat(t.name, '_1')"},
                {"book.name = 'Lord of the Rings'", "t.name = 'Lord of the Rings'"},
        };
    }

    @Test(dataProvider = "lambdaExpression")
    public void testLambdaExpressionRewrite(String actual, String expected)
    {
        Node node = LambdaExpressionBodyRewrite.rewrite(parse(actual), "Book", new Identifier("book"));
        assertThat(node.toString()).isEqualTo(parse(expected).toString());
    }

    private Expression parse(String sql)
    {
        return SQL_PARSER.createExpression(sql, new ParsingOptions(AS_DECIMAL));
    }
}
