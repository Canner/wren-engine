package io.accio.base.metadata;

import io.accio.base.pgcatalog.function.DataSourceFunctionRegistry;
import io.accio.base.pgcatalog.function.PgFunction;
import io.accio.base.pgcatalog.function.PgFunctionRegistry;
import io.accio.base.pgcatalog.function.PgMetastoreFunctionRegistry;

import java.util.Optional;

public class FunctionBundle
{
    private static final PgFunctionRegistry pgFunctionRegistry;
    private static final PgFunctionRegistry trinoFunctionRegistry;

    private FunctionBundle() {}

    static {
        pgFunctionRegistry = new PgMetastoreFunctionRegistry();
        trinoFunctionRegistry = new DataSourceFunctionRegistry();
    }

    public static Optional<PgFunction> getPgFunction(String name, int numArgument)
    {
        return pgFunctionRegistry.getPgFunction(name, numArgument)
                .or(() -> trinoFunctionRegistry.getPgFunction(name, numArgument));
    }
}
