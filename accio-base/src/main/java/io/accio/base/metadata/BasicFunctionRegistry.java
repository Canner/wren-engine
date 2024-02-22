package io.accio.base.metadata;

import com.google.common.collect.ImmutableList;
import io.accio.base.pgcatalog.function.FunctionRegistry;
import io.accio.base.type.PGType;

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
        functions.forEach(function -> simpleNameToFunction.put(functionKey(function.getName(), function.getArguments().map(this::getArgumentTypes).orElse(ImmutableList.of())), function));
    }

    public List<Function> getFunctions()
    {
        return functions;
    }

    @Override
    public Optional<Function> getFunction(String name, List<PGType<?>> argumentTypes)
    {
        return Optional.ofNullable(simpleNameToFunction.get(functionKey(name, argumentTypes)));
    }
}
