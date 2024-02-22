package io.accio.base.metadata;

import io.accio.base.pgcatalog.function.DataSourceFunctionRegistry;
import io.accio.base.pgcatalog.function.PgMetastoreFunctionRegistry;
import io.accio.base.type.PGType;

import java.util.List;
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

    public static Optional<Function> getFunction(String name, List<PGType<?>> argumentTypes)
    {
        return basicFunctionRegistry.getFunction(name, argumentTypes)
                .or(() -> pgMetastoreFunctionRegistry.getFunction(name, argumentTypes))
                .or(() -> datSourceFunctionRegistry.getFunction(name, argumentTypes));
    }
}
