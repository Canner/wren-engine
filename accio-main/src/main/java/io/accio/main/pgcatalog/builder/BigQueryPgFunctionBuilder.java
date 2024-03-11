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

package io.accio.main.pgcatalog.builder;

import io.accio.base.AccioException;
import io.accio.base.pgcatalog.function.PgFunction;
import io.accio.main.metadata.Metadata;

import java.util.Locale;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.main.pgcatalog.builder.BigQueryUtils.toBqType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BigQueryPgFunctionBuilder
        implements PgFunctionBuilder
{
    private final String pgCatalogName;

    public BigQueryPgFunctionBuilder(Metadata connector)
    {
        this.pgCatalogName = requireNonNull(connector.getPgCatalogName());
    }

    @Override
    public String generateCreateFunction(PgFunction pgFunction)
    {
        switch (pgFunction.getLanguage()) {
            case SQL:
                return generateCreateSqlFunction(pgFunction);
            case JS:
                return generateCreateJsFunction(pgFunction);
        }
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Unsupported language: " + pgFunction.getLanguage());
    }

    private String generateCreateSqlFunction(PgFunction pgFunction)
    {
        StringBuilder parameterBuilder = new StringBuilder();
        if (pgFunction.getArguments().isPresent()) {
            for (PgFunction.Argument argument : pgFunction.getArguments().get()) {
                parameterBuilder
                        .append(argument.getName()).append(" ")
                        .append(toBqType(argument.getType())).append(",");
            }
            parameterBuilder.setLength(parameterBuilder.length() - 1);
        }

        return format("CREATE OR REPLACE FUNCTION %s.%s(%s) AS ((%s))", pgCatalogName, pgFunction.getRemoteName(), parameterBuilder, pgFunction.getDefinition());
    }

    private String generateCreateJsFunction(PgFunction pgFunction)
    {
        StringBuilder parameterBuilder = new StringBuilder();
        if (pgFunction.getArguments().isPresent()) {
            for (PgFunction.Argument argument : pgFunction.getArguments().get()) {
                parameterBuilder
                        .append(argument.getName()).append(" ")
                        .append(toBqType(argument.getType())).append(",");
            }
            parameterBuilder.setLength(parameterBuilder.length() - 1);
        }

        return format("CREATE OR REPLACE FUNCTION %s.%s(%s) RETURNS %s LANGUAGE %s AS r\"\"\"%s\"\"\"",
                pgCatalogName,
                pgFunction.getRemoteName(),
                parameterBuilder,
                toBqType(pgFunction.getReturnType().orElse(VARCHAR)),
                pgFunction.getLanguage().name().toLowerCase(Locale.ROOT),
                pgFunction.getDefinition());
    }
}
