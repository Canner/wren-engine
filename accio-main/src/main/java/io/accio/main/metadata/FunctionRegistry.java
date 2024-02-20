package io.accio.main.metadata;

import io.accio.base.metadata.Function;

import java.util.Optional;

public interface FunctionRegistry
{
    Optional<Function> getFunction(String name, int numArgument);
}
