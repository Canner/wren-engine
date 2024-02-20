package io.accio.base.metadata;

import io.accio.base.pgcatalog.function.PgFunctionRegistry;

import java.util.Optional;

public class FunctionBundle
{
    private final FunctionRegistry pgFunctionRegistry;
    private final FunctionRegistry trinoFunctionRegistry;

    private FunctionBundle(FunctionRegistry pgFunctionRegistry, FunctionRegistry trinoFunctionRegistry)
    {
        this.pgFunctionRegistry = pgFunctionRegistry;
        this.trinoFunctionRegistry = trinoFunctionRegistry;
    }

    public static FunctionBundle create(String catalog)
    {
        return FunctionBundle.builder()
                .pgFunctions(new PgFunctionRegistry(catalog))
                .trinoFunctions(new PgFunctionRegistry(catalog)) // TODO: Add trino function registry instead it
                .build();
    }

    public Optional<Function> getFunction(String name, int numArgument)
    {
        return pgFunctionRegistry.getFunction(name, numArgument)
                .or(() -> trinoFunctionRegistry.getFunction(name, numArgument));
    }

    public static FunctionBundleBuilder builder()
    {
        return new FunctionBundleBuilder();
    }

    public static class FunctionBundleBuilder
    {
        private FunctionRegistry pgFunctionRegistry;
        private FunctionRegistry trinoFunctionRegistry;

        private FunctionBundleBuilder() {}

        public FunctionBundleBuilder pgFunctions(FunctionRegistry pgFunctionRegistry)
        {
            this.pgFunctionRegistry = pgFunctionRegistry;
            return this;
        }

        public FunctionBundleBuilder trinoFunctions(FunctionRegistry trinoFunctionRegistry)
        {
            this.trinoFunctionRegistry = trinoFunctionRegistry;
            return this;
        }

        public FunctionBundle build()
        {
            return new FunctionBundle(pgFunctionRegistry, trinoFunctionRegistry);
        }
    }
}
