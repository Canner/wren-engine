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
import io.accio.main.wireprotocol.PgMetastore;

import javax.inject.Inject;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;

public class DuckDBFunctionBuilder
        extends PgFunctionBuilder
        implements PgMetastoreFunctionBuilder
{
    @Inject
    public DuckDBFunctionBuilder(PgMetastore connector)
    {
        super((Metadata) connector);
    }

    @Override
    protected String generateCreateFunction(PgFunction pgFunction)
    {
        switch (pgFunction.getLanguage()) {
            case SQL:
                return generateCreateSqlFunction(pgFunction);
        }
        throw new AccioException(GENERIC_INTERNAL_ERROR, "Unsupported language: " + pgFunction.getLanguage());
    }

    private String generateCreateSqlFunction(PgFunction pgFunction)
    {
        StringBuilder parameterBuilder = new StringBuilder();
        if (pgFunction.getArguments().isPresent()) {
            for (PgFunction.Argument argument : pgFunction.getArguments().get()) {
                parameterBuilder
                        .append(argument.getName())
                        .append(", ");
            }
            parameterBuilder.delete(parameterBuilder.length() - 2, parameterBuilder.length());
        }

        return format("CREATE OR REPLACE MACRO %s(%s) AS (%s);",
                pgFunction.getName(),
                parameterBuilder,
                pgFunction.getDefinition());
    }
}
