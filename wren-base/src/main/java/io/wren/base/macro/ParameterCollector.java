/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.wren.base.macro;

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
