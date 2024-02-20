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

package io.accio.main.pgcatalog.function;

import com.google.common.collect.ImmutableList;
import io.accio.base.metadata.Function;
import io.accio.main.metadata.FunctionRegistry;

import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.accio.main.pgcatalog.function.PgFunctionRegistry.FunctionKey.functionKey;
import static io.accio.main.pgcatalog.function.PgFunctions.ARRAY_IN;
import static io.accio.main.pgcatalog.function.PgFunctions.ARRAY_OUT;
import static io.accio.main.pgcatalog.function.PgFunctions.ARRAY_RECV;
import static io.accio.main.pgcatalog.function.PgFunctions.ARRAY_UPPER;
import static io.accio.main.pgcatalog.function.PgFunctions.CURRENT_DATABASE;
import static io.accio.main.pgcatalog.function.PgFunctions.CURRENT_SCHEMAS;
import static io.accio.main.pgcatalog.function.PgFunctions.FORMAT_TYPE;
import static io.accio.main.pgcatalog.function.PgFunctions.NOW;
import static io.accio.main.pgcatalog.function.PgFunctions.PG_EXPANDARRAY;
import static io.accio.main.pgcatalog.function.PgFunctions.PG_GET_EXPR;
import static io.accio.main.pgcatalog.function.PgFunctions.PG_GET_EXPR_PRETTY;
import static io.accio.main.pgcatalog.function.PgFunctions.PG_GET_FUNCTION_RESULT;
import static io.accio.main.pgcatalog.function.PgFunctions.PG_RELATION_SIZE__INT_VARCHAR___BIGINT;
import static io.accio.main.pgcatalog.function.PgFunctions.PG_RELATION_SIZE__INT___BIGINT;
import static io.accio.main.pgcatalog.function.PgFunctions.PG_TO_CHAR;
import static io.accio.main.pgcatalog.function.PgFunctions.SUBSTR;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class PgFunctionRegistry
        implements FunctionRegistry
{
    private final List<PgFunction> pgFunctions;
    private final Map<FunctionKey, PgFunction> simpleNameToFunction = new HashMap<>();

    public PgFunctionRegistry(String pgCatalogName)
    {
        requireNonNull(pgCatalogName);
        pgFunctions = ImmutableList.<PgFunction>builder()
                .add(CURRENT_DATABASE)
                .add(CURRENT_SCHEMAS)
                .add(PG_RELATION_SIZE__INT___BIGINT)
                .add(PG_RELATION_SIZE__INT_VARCHAR___BIGINT)
                .add(ARRAY_IN)
                .add(ARRAY_OUT)
                .add(ARRAY_RECV)
                .add(ARRAY_UPPER)
                .add(PG_GET_EXPR)
                .add(PG_GET_EXPR_PRETTY)
                .add(FORMAT_TYPE.apply(pgCatalogName))
                .add(PG_GET_FUNCTION_RESULT.apply(pgCatalogName))
                .add(PG_TO_CHAR)
                .add(NOW)
                .add(PG_EXPANDARRAY)
                .add(SUBSTR)
                .build();

        // TODO: handle function name overloading
        //  https://github.com/Canner/canner-metric-layer/issues/73
        // use HashMap to handle multiple same key entries
        pgFunctions.forEach(pgFunction -> simpleNameToFunction.put(functionKey(pgFunction.getName(), pgFunction.getArguments().map(List::size).orElse(0)), pgFunction));
    }

    public List<PgFunction> getPgFunctions()
    {
        return pgFunctions;
    }

    @Override
    public Optional<Function> getFunction(String name, int numArgument)
    {
        return Optional.ofNullable(simpleNameToFunction.get(functionKey(name, numArgument)));
    }

    /**
     * TODO: analyze the type of argument expression
     *  https://github.com/Canner/canner-metric-layer/issues/92
     * <p>
     * We only support function overloading with different number of argument now. Because
     * the work of analyze the type of argument is too huge to implement, FunctionKey only
     * recognizes each function by its name and number of argument.
     */
    static class FunctionKey
    {
        public static FunctionKey functionKey(String name, int numArgument)
        {
            return new FunctionKey(name, numArgument);
        }

        private final String name;
        private final int numArgument;

        private FunctionKey(String name, int numArgument)
        {
            this.name = name;
            this.numArgument = numArgument;
        }

        public String getName()
        {
            return name;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, numArgument);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionKey that = (FunctionKey) o;
            return numArgument == that.numArgument && Objects.equals(name, that.name);
        }
    }
}
