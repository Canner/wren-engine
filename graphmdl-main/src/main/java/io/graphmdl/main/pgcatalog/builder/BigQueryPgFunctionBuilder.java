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

package io.graphmdl.main.pgcatalog.builder;

import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.main.pgcatalog.function.PgFunction;
import io.graphmdl.spi.GraphMDLException;

import javax.inject.Inject;

import java.util.Locale;

import static io.graphmdl.main.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static io.graphmdl.main.pgcatalog.builder.BigQueryUtils.toBqType;
import static io.graphmdl.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.graphmdl.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class BigQueryPgFunctionBuilder
        extends PgFunctionBuilder
{
    @Inject
    public BigQueryPgFunctionBuilder(Metadata connector)
    {
        super(connector);
    }

    @Override
    protected String generateCreateFunction(PgFunction pgFunction)
    {
        switch (pgFunction.getLanguage()) {
            case SQL:
                return generateCreateSqlFunction(pgFunction);
            case JS:
                return generateCreateJsFunction(pgFunction);
        }
        throw new GraphMDLException(GENERIC_INTERNAL_ERROR, "Unsupported language: " + pgFunction.getLanguage());
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

        return format("CREATE OR REPLACE FUNCTION %s.%s(%s) AS ((%s))", PG_CATALOG_NAME, pgFunction.getRemoteName(), parameterBuilder, pgFunction.getDefinition());
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
                PG_CATALOG_NAME,
                pgFunction.getRemoteName(),
                parameterBuilder,
                toBqType(pgFunction.getReturnType().orElse(VARCHAR)),
                pgFunction.getLanguage().name().toLowerCase(Locale.ROOT),
                pgFunction.getDefinition());
    }
}
