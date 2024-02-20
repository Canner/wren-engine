package io.accio.base.metadata;

import java.util.Optional;

public interface FunctionBundle
{
    Optional<Function> getFunction(String name, int numArgument);
}
