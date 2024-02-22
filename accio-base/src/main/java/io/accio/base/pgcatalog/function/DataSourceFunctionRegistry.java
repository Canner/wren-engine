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
import io.accio.base.metadata.FunctionKey;
import io.accio.base.type.PGType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.accio.base.metadata.FunctionKey.functionKey;
import static io.accio.base.pgcatalog.function.BigQueryFunctions.NOW;
import static io.accio.base.pgcatalog.function.BigQueryFunctions.PG_TO_CHAR;
import static io.accio.base.pgcatalog.function.BigQueryFunctions.SUBSTR;

public class DataSourceFunctionRegistry
        implements FunctionRegistry<PgFunction>
{
    private final List<PgFunction> functions = ImmutableList.<PgFunction>builder()
            .add(PG_TO_CHAR)
            .add(NOW)
            .add(SUBSTR)
            .build();

    private final Map<FunctionKey, PgFunction> simpleNameToFunction = new HashMap<>();

    public DataSourceFunctionRegistry()
    {
        functions.forEach(function -> simpleNameToFunction.put(functionKey(function.getName(), function.getArguments().map(this::getArgumentTypes).orElse(ImmutableList.of())), function));
    }

    @Override
    public List<PgFunction> getFunctions()
    {
        return functions;
    }

    @Override
    public Optional<PgFunction> getFunction(String name, List<PGType<?>> argumentTypes)
    {
        return Optional.ofNullable(simpleNameToFunction.get(functionKey(name, argumentTypes)));
    }
}
