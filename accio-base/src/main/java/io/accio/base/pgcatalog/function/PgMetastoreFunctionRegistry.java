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

package io.accio.base.pgcatalog.function;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.accio.base.pgcatalog.function.DuckDBFunctions.ARRAY_IN;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.ARRAY_OUT;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.ARRAY_RECV;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.ARRAY_UPPER;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.CURRENT_DATABASE;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.CURRENT_SCHEMAS;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.FORMAT_TYPE;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.GENERATE_ARRAY;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.PG_EXPANDARRAY;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.PG_GET_EXPR;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.PG_GET_EXPR_PRETTY;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.PG_GET_FUNCTION_RESULT;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.PG_RELATION_SIZE__INT_VARCHAR___BIGINT;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.PG_RELATION_SIZE__INT___BIGINT;
import static io.accio.base.pgcatalog.function.DuckDBFunctions.REGEXP_LIKE;
import static io.accio.base.pgcatalog.function.FunctionKey.functionKey;

public final class PgMetastoreFunctionRegistry
        implements PgFunctionRegistry
{
    public final List<PgFunction> pgFunctions = ImmutableList.<PgFunction>builder()
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
        pgFunctions.forEach(pgFunction -> simpleNameToFunction.put(functionKey(pgFunction.getName(), pgFunction.getArguments().map(List::size).orElse(0)), pgFunction));
    }

    public List<PgFunction> getPgFunctions()
    {
        return pgFunctions;
    }

    public Optional<PgFunction> getPgFunction(String name, int numArgument)
    {
        return Optional.ofNullable(simpleNameToFunction.get(functionKey(name, numArgument)));
    }
}
