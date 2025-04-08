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

package io.wren.base.jinjava;

import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;
import io.wren.base.WrenException;
import io.wren.base.dto.Macro;
import io.wren.base.macro.Parameter;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.SqlFormatter.formatSql;
import static io.wren.base.metadata.StandardErrorCode.SYNTAX_ERROR;
import static io.wren.base.sqlrewrite.Utils.parseExpression;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Process passing jinjava macro as an argument to another jinjava macro
 */
public class JinjavaExpressionProcessor
{
    public static String process(String source, List<Macro> macros)
    {
        return new JinjavaExpressionProcessor(source, null, macros).processInternal();
    }

    static String process(String source, CallerInfo caller, List<Macro> macros)
    {
        return new JinjavaExpressionProcessor(source, caller, macros).processInternal();
    }

    private final String source;
    private final StringBuilder resultBuffer;

    private final List<Macro> macros;

    private final Optional<CallerInfo> callerInfo;
    private StringBuilder expressionBuffer;
    private boolean inQuoted;
    private boolean inDoubleQuoted;
    private boolean inExpression;

    public JinjavaExpressionProcessor(String source, CallerInfo callerInfo, List<Macro> macros)
    {
        this.source = requireNonNull(source, "source is null");
        this.callerInfo = Optional.ofNullable(callerInfo);
        this.macros = macros == null ? List.of() : macros;
        this.resultBuffer = new StringBuilder(source.length());
        this.expressionBuffer = new StringBuilder();
    }

    private String processInternal()
    {
        for (int i = 0; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{') {
                if (i + 1 < source.length() && source.charAt(i + 1) == '{') {
                    i++;
                    if (inQuoted || inDoubleQuoted) {
                        resultBuffer.append(c);
                    }
                    else if (inExpression) {
                        throw new WrenException(SYNTAX_ERROR, format("Generating macro failed: Nested expression is not supported for macro: %s", source));
                    }
                    else {
                        inExpression = true;
                    }
                }
                else {
                    resultBuffer.append(c);
                }
            }
            else if (c == '}') {
                if (i + 1 < source.length() && source.charAt(i + 1) == '}') {
                    i++;
                    if (inQuoted || inDoubleQuoted) {
                        resultBuffer.append(c);
                    }
                    else if (!inExpression) {
                        throw new WrenException(SYNTAX_ERROR, format("Generating macro failed: Unmatched }} in %s", source));
                    }
                    else {
                        inExpression = false;
                        resultBuffer.append(processExpression(expressionBuffer.toString()));
                        expressionBuffer = new StringBuilder();
                    }
                }
                else {
                    resultBuffer.append(c);
                }
            }
            else if (c == '\'') {
                if (inDoubleQuoted) {
                    if (inExpression) {
                        expressionBuffer.append(c);
                    }
                    else {
                        resultBuffer.append(c);
                    }
                }
                else if (inExpression) {
                    inQuoted = !inQuoted;
                    expressionBuffer.append(c);
                }
                else {
                    inQuoted = !inQuoted;
                    resultBuffer.append(c);
                }
            }
            else if (c == '"') {
                if (inQuoted) {
                    if (inExpression) {
                        expressionBuffer.append(c);
                    }
                    else {
                        resultBuffer.append(c);
                    }
                }
                else if (inExpression) {
                    inDoubleQuoted = !inDoubleQuoted;
                    expressionBuffer.append(c);
                }
                else {
                    inDoubleQuoted = !inDoubleQuoted;
                    resultBuffer.append(c);
                }
            }
            else if (inExpression) {
                expressionBuffer.append(c);
            }
            else {
                resultBuffer.append(c);
            }
        }
        return resultBuffer.toString();
    }

    private String processExpression(String expression)
    {
        Expression macroExpression = parseExpression(expression);
        return Optional.ofNullable(new Processor(macros, callerInfo).process(macroExpression, null))
                .orElseThrow(() -> new WrenException(SYNTAX_ERROR, format("Failed to apply macro to %s in %s", expression, source)));
    }

    static class Processor
            extends AstVisitor<String, Void>
    {
        private final List<Macro> macros;
        private Optional<CallerInfo> callerInfo = Optional.empty();

        public Processor(List<Macro> macros, Optional<CallerInfo> callerInfo)
        {
            this.macros = macros;
            this.callerInfo = callerInfo;
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context)
        {
            return format("{{ %s }}", formatSql(node));
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            String functionName = node.getName().toString();
            List<Expression> arguments = node.getArguments();
            Optional<Macro> callee = macros.stream()
                    .filter(m -> m.getName().equals(functionName))
                    .filter(m -> m.getParameters().stream().anyMatch(p -> p.getType() == Parameter.TYPE.MACRO))
                    .findAny();
            if (callee.isPresent()) {
                return JinjavaExpressionProcessor.process(
                        callee.get().getBody(),
                        new CallerInfo(callee.get(), arguments),
                        macros);
            }

            if (callerInfo.isPresent()) {
                Macro caller = callerInfo.get().getCaller();
                Expression processed = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
                {
                    @Override
                    public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                    {
                        Optional<Parameter> matched = caller.getParameters().stream()
                                .filter(p -> p.getName().equals(node.getName().toString()) && p.getType() == Parameter.TYPE.MACRO)
                                .findAny();
                        if (matched.isEmpty()) {
                            return treeRewriter.defaultRewrite(node, context);
                        }
                        int index = caller.getParameters().indexOf(matched.get());
                        if (node.getLocation().isPresent()) {
                            return treeRewriter.defaultRewrite(new FunctionCall(
                                            node.getLocation().get(),
                                            QualifiedName.of(callerInfo.get().getArguments().get(index).toString()),
                                            node.getArguments().stream()
                                                    .map(expression -> treeRewriter.defaultRewrite(expression, context))
                                                    .collect(toList())),
                                    context);
                        }
                        return treeRewriter.defaultRewrite(
                                new FunctionCall(QualifiedName.of(callerInfo.get().getArguments().get(index).toString()), node.getArguments().stream()
                                        .map(expression -> treeRewriter.defaultRewrite(expression, context))
                                        .collect(toList())),
                                context);
                    }

                    @Override
                    public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                    {
                        Optional<Parameter> matched = caller.getParameters().stream()
                                .filter(p -> p.getName().equals(node.getValue()) && p.getType() == Parameter.TYPE.EXPRESSION)
                                .findAny();
                        if (matched.isEmpty()) {
                            return treeRewriter.defaultRewrite(node, context);
                        }

                        int index = caller.getParameters().indexOf(matched.get());
                        Expression newValue = callerInfo.get().getArguments().get(index);

                        if (node.getLocation().isPresent()) {
                            return treeRewriter.defaultRewrite(
                                    newValue,
                                    context);
                        }
                        return treeRewriter.defaultRewrite(
                                newValue,
                                context);
                    }
                }, node);
                return format("{{ %s }}", formatSql(processed));
            }
            return format("{{ %s }}", formatSql(node));
        }
    }

    static class CallerInfo
    {
        private final Macro caller;
        private final List<Expression> arguments;

        public CallerInfo(Macro caller, List<Expression> arguments)
        {
            this.caller = caller;
            this.arguments = arguments;
        }

        public Macro getCaller()
        {
            return caller;
        }

        public List<Expression> getArguments()
        {
            return arguments;
        }
    }
}
