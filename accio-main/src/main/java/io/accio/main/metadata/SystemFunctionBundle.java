package io.accio.main.metadata;

import io.accio.base.metadata.Function;
import io.accio.base.metadata.FunctionBundle;
import io.accio.main.pgcatalog.function.PgFunctionRegistry;

import java.util.Optional;

public class SystemFunctionBundle
    implements FunctionBundle
{
    private final FunctionRegistry pgFunctionRegistry;
    private final FunctionRegistry trinoFunctionRegistry;

    private SystemFunctionBundle(FunctionRegistry pgFunctionRegistry, FunctionRegistry trinoFunctionRegistry)
    {
        this.pgFunctionRegistry = pgFunctionRegistry;
        this.trinoFunctionRegistry = trinoFunctionRegistry;
    }

    public static FunctionBundle create(String pgCatalogName)
    {
        return SystemFunctionBundle.builder()
                .pgFunctions(new PgFunctionRegistry(pgCatalogName))
                .trinoFunctions(new PgFunctionRegistry(pgCatalogName)) // TODO: Add trino function registry instead it
                .build();
    }

    @Override
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

        public SystemFunctionBundle build()
        {
            return new SystemFunctionBundle(pgFunctionRegistry, trinoFunctionRegistry);
        }
    }
}
