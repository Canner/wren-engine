package io.accio.base.macro;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.List;

import static java.util.Locale.ENGLISH;

public class ParameterCollector
        extends ParameterListBaseBaseVisitor<Void>
{
    private final List<Parameter> parameters = new ArrayList<>();

    public List<Parameter> collect(ParserRuleContext context)
    {
        visit(context);
        return parameters;
    }

    @Override
    public Void visitParameter(ParameterListBaseParser.ParameterContext ctx)
    {
        parameters.add(new Parameter(ctx.paraName().getText(),
                Parameter.TYPE.valueOf(ctx.typeName().getText().toUpperCase(ENGLISH))));
        return null;
    }
}
