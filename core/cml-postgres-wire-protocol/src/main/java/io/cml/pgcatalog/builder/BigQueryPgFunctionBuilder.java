/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cml.pgcatalog.builder;

import io.cml.pgcatalog.function.PgFunction;
import io.cml.spi.CmlException;
import io.cml.spi.connector.Connector;

import javax.inject.Inject;

import java.util.Locale;

import static io.cml.pgcatalog.PgCatalogUtils.PG_CATALOG_NAME;
import static io.cml.pgcatalog.builder.BigQueryUtils.getOidToBqType;
import static io.cml.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.cml.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class BigQueryPgFunctionBuilder
        extends PgFunctionBuilder
{
    @Inject
    public BigQueryPgFunctionBuilder(Connector connector)
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
        throw new CmlException(GENERIC_INTERNAL_ERROR, "Unsupported language: " + pgFunction.getLanguage());
    }

    private String generateCreateSqlFunction(PgFunction pgFunction)
    {
        StringBuilder parameterBuilder = new StringBuilder();
        if (pgFunction.getArguments().isPresent()) {
            for (PgFunction.Argument argument : pgFunction.getArguments().get()) {
                parameterBuilder
                        .append(argument.getName()).append(" ")
                        .append(getOidToBqType().get(argument.getType().oid())).append(",");
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
                        .append(getOidToBqType().get(argument.getType().oid())).append(",");
            }
            parameterBuilder.setLength(parameterBuilder.length() - 1);
        }

        return format("CREATE OR REPLACE FUNCTION %s.%s(%s) RETURNS %s LANGUAGE %s AS r\"\"\"%s\"\"\"",
                PG_CATALOG_NAME,
                pgFunction.getRemoteName(),
                parameterBuilder,
                getOidToBqType().get(pgFunction.getReturnType().orElse(VARCHAR).oid()),
                pgFunction.getLanguage().name().toLowerCase(Locale.ROOT),
                pgFunction.getDefinition());
    }
}
