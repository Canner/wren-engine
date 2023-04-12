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

import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.sqlrewrite.analyzer.Field;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

public class TestLambdaExpressionRewrite
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @DataProvider
    public Object[][] lambdaExpression()
    {
        // TODO: support FunctionCall, ComparisonExpression ...
        return new Object[][] {
                {"book.f1.f2.f3"},
                {"book"},
                {"book.f1.a1[1].f2"}
        };
    }

    @Test(dataProvider = "lambdaExpression")
    public void testLambdaExpressionRewrite(String sql)
    {
        CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName("graphmdl", "test", "Book");
        Node node = LambdaExpressionRewrite.rewrite(getSelectItem(String.format("select %s", sql)),
                Field.builder()
                        .isRelationship(true)
                        .modelName(catalogSchemaTableName)
                        .columnName("books")
                        .name("books")
                        .relationAlias(QualifiedName.of("t"))
                        .build(), new Identifier("book"));
        System.out.println(node);
    }

    private Expression getSelectItem(String sql)
    {
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        return ((SingleColumn) ((QuerySpecification) ((Query) statement).getQueryBody()).getSelect().getSelectItems().get(0)).getExpression();
    }
}
