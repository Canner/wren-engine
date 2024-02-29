package io.accio.base.metadata;

import io.accio.base.type.PGType;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.metadata.FunctionKey.functionKey;
import static java.util.Objects.requireNonNull;

public class Function
{
    protected final String name;
    protected final List<Argument> arguments;
    protected final PGType returnType;
    protected final FunctionKey functionKey;

    public Function(String name, List<Argument> arguments, PGType returnType)
    {
        this.name = name;
        this.arguments = arguments;
        this.returnType = returnType;
        this.functionKey = functionKey(name, getArgumentTypes());
    }

    public String getName()
    {
        return name;
    }

    public Optional<List<Argument>> getArguments()
    {
        return Optional.ofNullable(arguments);
    }

    public Optional<PGType> getReturnType()
    {
        return Optional.ofNullable(returnType);
    }

    public FunctionKey getFunctionKey()
    {
        return functionKey;
    }

    private List<PGType<?>> getArgumentTypes()
    {
        return getArguments().orElse(List.of()).stream().map(Argument::getType).collect(toImmutableList());
    }

    public static class Argument
    {
        public static Argument argument(String name, PGType<?> type)
        {
            return new Argument(name, type);
        }

        private final String name;
        private final PGType<?> type;

        public Argument(String name, PGType<?> type)
        {
            this.name = name;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public PGType<?> getType()
        {
            return type;
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String name;
        private List<Argument> arguments;
        private PGType returnType;

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder setArguments(List<Argument> arguments)
        {
            this.arguments = arguments;
            return this;
        }

        public Builder setReturnType(PGType returnType)
        {
            this.returnType = returnType;
            return this;
        }

        public Function build()
        {
            requireNonNull(name, "name is null");
            return new Function(name, arguments, returnType);
        }
    }
}
