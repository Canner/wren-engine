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

package io.accio.sqlrewrite.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.accio.base.dto.Relationship;
import io.accio.sqlrewrite.LambdaExpressionBodyRewrite;
import io.accio.sqlrewrite.RelationshipCteGenerator;
import io.accio.sqlrewrite.analyzer.ExpressionAnalyzer.RelationshipField;
import io.accio.sqlrewrite.analyzer.ExpressionAnalyzer.ReplaceNodeInfo;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;

import static io.accio.base.Utils.checkArgument;
import static io.accio.sqlrewrite.RelationshipCteGenerator.LAMBDA_RESULT_NAME;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RelationshipOperation.aggregate;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RelationshipOperation.arraySort;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RelationshipOperation.filter;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RelationshipOperation.slice;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RelationshipOperation.transform;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RsItem.Type.CTE;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RsItem.Type.REVERSE_RS;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RsItem.Type.RS;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RsItem.rsItem;
import static io.accio.sqlrewrite.RelationshipCteGenerator.SOURCE_REFERENCE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public interface FunctionChainAnalyzer
{
    Optional<ReturnContext> analyze(FunctionCall functionCall);

    static FunctionChainAnalyzer of(
            RelationshipCteGenerator relationshipCteGenerator,
            Function<Expression, Optional<ReplaceNodeInfo>> registerRelationshipCTEs)
    {
        return functionCall -> new FunctionChainProcessor(relationshipCteGenerator, registerRelationshipCTEs)
                .process(functionCall, new Context());
    }

    class Context
    {
        private final List<FunctionCall> functionChain;

        private Context()
        {
            this.functionChain = List.of();
        }

        private Context(List<FunctionCall> functionChain)
        {
            this.functionChain = requireNonNull(functionChain);
        }

        public List<FunctionCall> getFunctionChain()
        {
            return functionChain;
        }
    }

    class ReturnContext
    {
        private final Map<NodeRef<Expression>, RelationshipField> nodesToReplace;

        private ReturnContext(Map<NodeRef<Expression>, RelationshipField> nodesToReplace)
        {
            this.nodesToReplace = requireNonNull(nodesToReplace);
        }

        public Map<NodeRef<Expression>, RelationshipField> getNodesToReplace()
        {
            return nodesToReplace;
        }
    }

    class FunctionChainProcessor
            extends AstVisitor<Optional<ReturnContext>, Context>
    {
        private final RelationshipCteGenerator relationshipCteGenerator;
        private final Function<Expression, Optional<ReplaceNodeInfo>> registerRelationshipCTEs;

        private FunctionChainProcessor(
                RelationshipCteGenerator relationshipCteGenerator,
                Function<Expression, Optional<ReplaceNodeInfo>> registerRelationshipCTEs)
        {
            this.relationshipCteGenerator = requireNonNull(relationshipCteGenerator);
            this.registerRelationshipCTEs = requireNonNull(registerRelationshipCTEs);
        }

        @Override
        protected Optional<ReturnContext> visitFunctionCall(FunctionCall node, Context context)
        {
            List<ReturnContext> returnContexts = new ArrayList<>();
            Context newContext = new Context(ImmutableList.<FunctionCall>builder().addAll(context.getFunctionChain()).add(node).build());
            for (Expression argument : node.getArguments()) {
                if (argument instanceof FunctionCall) {
                    visitFunctionCall((FunctionCall) argument, newContext).ifPresent(returnContexts::add);
                }
                else if (argument instanceof DereferenceExpression || argument instanceof Identifier) {
                    processFunctionChain(argument, newContext).ifPresent(returnContexts::add);
                }
            }

            Map<NodeRef<Expression>, RelationshipField> nodesToReplace = returnContexts.stream()
                    .map(returnContext -> returnContext.nodesToReplace.entrySet())
                    .flatMap(Collection::stream)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            Optional<Entry<NodeRef<Expression>, RelationshipField>> fullMatch = nodesToReplace.entrySet().stream()
                    .filter(entry -> NodeRef.of(node).equals(entry.getKey()))
                    .findAny();

            return Optional.of(
                    fullMatch
                            .map(match -> new ReturnContext(Map.of(match.getKey(), match.getValue())))
                            .orElseGet(() -> new ReturnContext(nodesToReplace)));
        }

        private Optional<ReturnContext> processFunctionChain(Expression node, Context context)
        {
            checkArgument(node instanceof DereferenceExpression || node instanceof Identifier, "node is not DereferenceExpression or Identifier");
            checkFunctionChainIsValid(context.getFunctionChain());

            Optional<ReplaceNodeInfo> replaceNodeInfo = registerRelationshipCTEs.apply(node);
            if (replaceNodeInfo.isEmpty()) {
                return Optional.empty();
            }

            checkArgument(replaceNodeInfo.get().getLastRelationshipField().isPresent(), "last relationship field not found in function call");
            RelationshipField rsField = replaceNodeInfo.get().getLastRelationshipField().get();

            FunctionCall previousFunctionCall = null;
            for (FunctionCall functionCall : Lists.reverse(context.getFunctionChain())) {
                if (isAccioFunction(functionCall.getName())) {
                    collectRelationshipInFunction(
                            functionCall,
                            rsField,
                            Optional.ofNullable(previousFunctionCall),
                            relationshipCteGenerator);
                }
                else if (isArrayFunction(functionCall.getName())) {
                    return Optional.of(new ReturnContext(Map.of(NodeRef.of(node), rsField)));
                }
                else {
                    break;
                }
                previousFunctionCall = functionCall;
            }
            return Optional.ofNullable(previousFunctionCall)
                    .map(functionCall -> new ReturnContext(Map.of(NodeRef.of(functionCall), rsField)));
        }
    }

    private static void checkFunctionChainIsValid(List<FunctionCall> functionCalls)
    {
        boolean startChaining = false;
        for (FunctionCall functionCall : functionCalls) {
            if (isAccioFunction(functionCall.getName())) {
                startChaining = true;
            }
            else {
                checkArgument(!startChaining, format("accio function chain contains invalid function %s", functionCall.getName()));
            }
        }
    }

    private static boolean isAccioFunction(QualifiedName funcName)
    {
        return isLambdaFunction(funcName)
                || funcName.getSuffix().equalsIgnoreCase("array_sort")
                || funcName.getSuffix().equalsIgnoreCase("slice");
    }

    private static boolean isLambdaFunction(QualifiedName funcName)
    {
        return List.of("transform", "filter").contains(funcName.getSuffix()) || isAggregateFunction(funcName);
    }

    private static boolean isAggregateFunction(QualifiedName funcName)
    {
        return List.of("array_count",
                        "array_sum",
                        "array_avg",
                        "array_min",
                        "array_max",
                        "array_bool_or",
                        "array_every")
                .contains(funcName.getSuffix());
    }

    private static boolean isArrayFunction(QualifiedName funcName)
    {
        // TODO: define what's array function
        //  Refer to trino array function temporarily
        // TODO: bigquery array function mapping
        return List.of("cardinality", "array_max", "array_min", "array_length").contains(funcName.toString());
    }

    private static void collectRelationshipInFunction(
            FunctionCall functionCall,
            RelationshipField relationshipField,
            Optional<FunctionCall> previousLambdaCall,
            RelationshipCteGenerator relationshipCteGenerator)
    {
        String modelName = relationshipField.getModelName();
        String columnName = relationshipField.getColumnName();
        Relationship relationship = relationshipField.getRelationship();

        RelationshipCteGenerator.RelationshipOperation operation;
        String functionName = functionCall.getName().toString();
        String cteName = previousLambdaCall.map(Expression::toString).orElse(String.join(".", relationshipField.getCteNameParts()));
        Expression unnestField = previousLambdaCall.isPresent() ? DereferenceExpression.from(QualifiedName.of(SOURCE_REFERENCE, LAMBDA_RESULT_NAME)) : null;
        List<Expression> arguments = functionCall.getArguments();
        if (isLambdaFunction(QualifiedName.of(functionName.toLowerCase(ENGLISH)))) {
            checkArgument(arguments.size() == 2, "Lambda function should have 2 arguments");
            LambdaExpression lambdaExpression = (LambdaExpression) functionCall.getArguments().get(1);
            checkArgument(lambdaExpression.getArguments().size() == 1, "lambda expression must have one argument");
            Expression expression = LambdaExpressionBodyRewrite.rewrite(lambdaExpression.getBody(), modelName, lambdaExpression.getArguments().get(0).getName());
            if (functionName.equalsIgnoreCase("transform")) {
                operation = transform(
                        List.of(rsItem(cteName, CTE),
                                rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RS : REVERSE_RS)),
                        expression,
                        columnName,
                        unnestField);
            }
            else if (functionName.equalsIgnoreCase("filter")) {
                operation = filter(
                        List.of(rsItem(cteName, CTE),
                                rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RS : REVERSE_RS)),
                        expression,
                        columnName,
                        unnestField);
            }
            else if (isAggregateFunction(QualifiedName.of(functionName))) {
                operation = aggregate(
                        List.of(rsItem(cteName, CTE),
                                rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RS : REVERSE_RS)),
                        expression,
                        columnName,
                        unnestField,
                        getArrayBaseFunctionName(functionName));
            }
            else {
                throw new IllegalArgumentException(functionName + " not supported");
            }
        }
        else if (functionName.equalsIgnoreCase("array_sort")) {
            operation = arraySort(
                    List.of(rsItem(cteName, CTE),
                            rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RS : REVERSE_RS)),
                    columnName,
                    unnestField,
                    arguments);
        }
        else if (functionName.equalsIgnoreCase("slice")) {
            checkArgument(arguments.size() == 3, "slice function should have 3 arguments");
            operation = slice(
                    List.of(rsItem(cteName, CTE),
                            rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RS : REVERSE_RS)),
                    previousLambdaCall.isPresent() ? LAMBDA_RESULT_NAME : columnName,
                    arguments);
        }
        else {
            throw new IllegalArgumentException(functionName + " not supported");
        }

        relationshipCteGenerator.register(List.of(functionCall.toString()), operation, relationshipField.getBaseModelName());
    }

    private static String getArrayBaseFunctionName(String functionName)
    {
        return functionName.split("array_")[1];
    }
}
