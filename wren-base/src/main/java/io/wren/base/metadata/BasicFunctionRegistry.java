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

package io.wren.base.metadata;

import com.google.common.collect.ImmutableList;
import io.wren.base.pgcatalog.function.FunctionRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BasicFunctionRegistry
        implements FunctionRegistry<Function>
{
    private final List<Function> functions = ImmutableList.<Function>builder()
            .add(BasicFunctions.DATE_TRUNC)
            .build();

    private final Map<FunctionKey, Function> simpleNameToFunction = new HashMap<>();

    public BasicFunctionRegistry()
    {
        // TODO: handle function name overloading
        //  https://github.com/Canner/canner-metric-layer/issues/73
        // use HashMap to handle multiple same key entries
        functions.forEach(function -> simpleNameToFunction.put(FunctionKey.functionKey(function.getName(), function.getArguments().map(List::size).orElse(0)), function));
    }

    public List<Function> getFunctions()
    {
        return functions;
    }

    public Optional<Function> getFunction(String name, int numArgument)
    {
        return Optional.ofNullable(simpleNameToFunction.get(FunctionKey.functionKey(name, numArgument)));
    }
}
