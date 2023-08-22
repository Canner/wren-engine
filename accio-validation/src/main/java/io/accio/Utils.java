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

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;

import java.util.Arrays;
import java.util.stream.Collectors;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

public class Utils
{
    private Utils() {}

    public static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = new ParsingOptions(AS_DECIMAL);

    public static Expression parseExpression(String expression)
    {
        return SQL_PARSER.createExpression(expression, PARSING_OPTIONS);
    }

    public static Expression removePrefix(DereferenceExpression dereferenceExpression, int removeFirstN)
    {
        return parseExpression(
                Arrays.stream(dereferenceExpression.toString().split("\\."))
                        .skip(removeFirstN)
                        .collect(Collectors.joining(".")));
    }
}
