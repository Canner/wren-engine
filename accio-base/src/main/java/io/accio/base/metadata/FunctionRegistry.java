package io.accio.base.metadata;

import java.util.Optional;

public interface FunctionRegistry
{
    Optional<Function> getFunction(String name, int numArgument);
}
