package io.accio.base.macro;

import java.util.Objects;

public class Parameter
{
    public static Parameter expressionType(String name)
    {
        return new Parameter(name, TYPE.EXPRESSION);
    }

    public static Parameter macroType(String name)
    {
        return new Parameter(name, TYPE.MACRO);
    }

    public enum TYPE
    {
        MACRO,
        EXPRESSION
    }

    private final String name;
    private final TYPE type;

    public Parameter(String name, TYPE type)
    {
        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public TYPE getType()
    {
        return type;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Parameter parameter = (Parameter) o;
        return Objects.equals(name, parameter.name) && type == parameter.type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public String toString()
    {
        return "Parameter{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }
}
