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

package io.wren.base.pgcatalog.function;

import com.google.common.collect.ImmutableList;
import io.wren.base.metadata.FunctionKey;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.wren.base.metadata.FunctionKey.functionKey;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.ARRAY_IN;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.ARRAY_OUT;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.ARRAY_RECV;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.ARRAY_UPPER;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.CURRENT_DATABASE;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.CURRENT_SCHEMAS;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.FORMAT_TYPE;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.GENERATE_ARRAY;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.PG_EXPANDARRAY;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.PG_GET_EXPR;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.PG_GET_EXPR_PRETTY;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.PG_GET_FUNCTION_RESULT;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.PG_RELATION_SIZE__INT_VARCHAR___BIGINT;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.PG_RELATION_SIZE__INT___BIGINT;
import static io.wren.base.pgcatalog.function.DuckDBFunctions.REGEXP_LIKE;

public final class PgMetastoreFunctionRegistry
        implements FunctionRegistry<PgFunction>
{
    public final List<PgFunction> functions = ImmutableList.<PgFunction>builder()
            .add(CURRENT_DATABASE)
            .add(CURRENT_SCHEMAS)
            .add(PG_RELATION_SIZE__INT___BIGINT)
            .add(PG_RELATION_SIZE__INT_VARCHAR___BIGINT)
            .add(ARRAY_IN)
            .add(ARRAY_OUT)
            .add(ARRAY_RECV)
            .add(ARRAY_UPPER)
            .add(FORMAT_TYPE)
            .add(PG_GET_FUNCTION_RESULT)
            .add(REGEXP_LIKE)
            .add(GENERATE_ARRAY)
            .add(PG_EXPANDARRAY)
            .add(PG_GET_EXPR)
            .add(PG_GET_EXPR_PRETTY)
            .build();

    private final Map<FunctionKey, PgFunction> simpleNameToFunction = new HashMap<>();

    public PgMetastoreFunctionRegistry()
    {
        // TODO: handle function name overloading
        //  https://github.com/Canner/canner-metric-layer/issues/73
        // use HashMap to handle multiple same key entries
        functions.forEach(function -> simpleNameToFunction.put(functionKey(function.getName(), function.getArguments().map(List::size).orElse(0)), function));
    }

    @Override
    public List<PgFunction> getFunctions()
    {
        return functions;
    }

    @Override
    public Optional<PgFunction> getFunction(String name, int numArgument)
    {
        return Optional.ofNullable(simpleNameToFunction.get(functionKey(name, numArgument)));
    }
}
