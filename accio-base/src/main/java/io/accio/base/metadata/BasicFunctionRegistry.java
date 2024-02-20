package io.accio.base.metadata;

import com.google.common.collect.ImmutableList;
import io.accio.base.pgcatalog.function.FunctionRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.accio.base.metadata.BasicFunctions.DATE_TRUNC;
import static io.accio.base.metadata.FunctionKey.functionKey;

public class BasicFunctionRegistry
        implements FunctionRegistry<Function>
{
    private final List<Function> functions = ImmutableList.<Function>builder()
            .add(DATE_TRUNC)
            .build();

    private final Map<FunctionKey, Function> simpleNameToFunction = new HashMap<>();

    public BasicFunctionRegistry()
    {
        // TODO: handle function name overloading
        //  https://github.com/Canner/canner-metric-layer/issues/73
        // use HashMap to handle multiple same key entries
        functions.forEach(function -> simpleNameToFunction.put(functionKey(function.getName(), function.getArguments().map(List::size).orElse(0)), function));
    }

    public List<Function> getFunctions()
    {
        return functions;
    }

    public Optional<Function> getFunction(String name, int numArgument)
    {
        return Optional.ofNullable(simpleNameToFunction.get(functionKey(name, numArgument)));
    }
}
