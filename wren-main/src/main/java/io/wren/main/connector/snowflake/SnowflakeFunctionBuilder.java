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

package io.wren.main.connector.snowflake;

import io.wren.base.WrenException;
import io.wren.base.pgcatalog.function.PgFunction;
import io.wren.main.pgcatalog.builder.PgFunctionBuilder;

import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.wren.base.pgcatalog.function.PgFunction.Language.SQL;
import static io.wren.main.connector.snowflake.SnowflakeType.toSFType;
import static java.lang.String.format;

public class SnowflakeFunctionBuilder
        implements PgFunctionBuilder
{
    @Override
    public String generateCreateFunction(PgFunction pgFunction)
    {
        if (pgFunction.getLanguage() == SQL) {
            return generateCreateSqlFunction(pgFunction);
        }
        throw new WrenException(GENERIC_INTERNAL_ERROR, "Unsupported language: " + pgFunction.getLanguage());
    }

    private String generateCreateSqlFunction(PgFunction pgFunction)
    {
        StringBuilder parameterBuilder = new StringBuilder();
        pgFunction.getArguments().ifPresent(arguments -> {
            for (PgFunction.Argument argument : arguments) {
                parameterBuilder
                        .append(argument.getName()).append(" ")
                        .append(toSFType(argument.getType()))
                        .append(",");
            }
            parameterBuilder.setLength(parameterBuilder.length() - 1);
        });
        return format("CREATE OR REPLACE FUNCTION %s(%s) AS $$ %s $$;",
                pgFunction.getName(),
                parameterBuilder,
                pgFunction.getDefinition());
    }
}
