package io.accio.base.metadata;

import io.accio.base.pgcatalog.function.DataSourceFunctionRegistry;
import io.accio.base.pgcatalog.function.PgMetastoreFunctionRegistry;

import java.util.Optional;

public class FunctionBundle
{
    private static final BasicFunctionRegistry basicFunctionRegistry;
    private static final PgMetastoreFunctionRegistry pgMetastoreFunctionRegistry;
    private static final DataSourceFunctionRegistry datSourceFunctionRegistry;

    private FunctionBundle() {}

    static {
        basicFunctionRegistry = new BasicFunctionRegistry();
        pgMetastoreFunctionRegistry = new PgMetastoreFunctionRegistry();
        datSourceFunctionRegistry = new DataSourceFunctionRegistry();
    }

    public static Optional<Function> getFunction(String name, int numArgument)
    {
        return basicFunctionRegistry.getFunction(name, numArgument)
                .or(() -> pgMetastoreFunctionRegistry.getFunction(name, numArgument))
                .or(() -> datSourceFunctionRegistry.getFunction(name, numArgument));
    }
}
