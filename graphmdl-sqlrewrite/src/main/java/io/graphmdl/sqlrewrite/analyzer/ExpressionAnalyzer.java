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

package io.graphmdl.sqlrewrite.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.dto.Column;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.Relationship;
import io.graphmdl.sqlrewrite.LambdaExpressionBodyRewrite;
import io.graphmdl.sqlrewrite.RelationshipCTE;
import io.graphmdl.sqlrewrite.RelationshipCteGenerator;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SubscriptExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.graphmdl.base.Utils.checkArgument;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.LAMBDA_RESULT_NAME;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.RelationshipOperation.access;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.RelationshipOperation.filter;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.RelationshipOperation.transform;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.RsItem.Type.CTE;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.RsItem.Type.REVERSE_RS;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.RsItem.Type.RS;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.RsItem.rsItem;
import static io.graphmdl.sqlrewrite.RelationshipCteGenerator.SOURCE_REFERENCE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ExpressionAnalyzer
{
    private ExpressionAnalyzer() {}

    private final Map<NodeRef<Expression>, Expression> relationshipFieldsRewrite = new HashMap<>();
    private final Set<String> relationshipCTENames = new HashSet<>();
    private final Set<Relationship> relationships = new HashSet<>();

    public static ExpressionAnalysis analyze(
            Expression expression,
            SessionContext sessionContext,
            GraphMDL graphMDL,
            RelationshipCteGenerator relationshipCteGenerator,
            Scope scope)
    {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer();
        return expressionAnalyzer.analyzeExpression(expression, sessionContext, graphMDL, relationshipCteGenerator, scope);
    }

    private ExpressionAnalysis analyzeExpression(
            Expression expression,
            SessionContext sessionContext,
            GraphMDL graphMDL,
            RelationshipCteGenerator relationshipCteGenerator,
            Scope scope)
    {
        new Visitor(sessionContext, graphMDL, relationshipCteGenerator, scope).process(expression);
        return new ExpressionAnalysis(expression, relationshipFieldsRewrite, relationshipCTENames, relationships);
    }

    private class Visitor
            extends AstVisitor<Void, Void>
    {
        private final SessionContext sessionContext;
        private final GraphMDL graphMDL;
        private final RelationshipCteGenerator relationshipCteGenerator;
        private final Scope scope;

        public Visitor(SessionContext sessionContext, GraphMDL graphMDL, RelationshipCteGenerator relationshipCteGenerator, Scope scope)
        {
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.graphMDL = requireNonNull(graphMDL, "graphMDL is null");
            this.relationshipCteGenerator = requireNonNull(relationshipCteGenerator, "relationshipCteGenerator is null");
            this.scope = requireNonNull(scope, "scope is null");
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void ignored)
        {
            process(node.getLeft());
            process(node.getRight());
            return ignored;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void ignored)
        {
            FunctionCallProcessor functionCallProcessor = new FunctionCallProcessor();
            functionCallProcessor.process(node, ignored);

            for (FunctionCall functionCall : functionCallProcessor.getReplaceFunctionCalls()) {
                relationshipCTENames.add(functionCall.toString());
                relationshipFieldsRewrite.put(
                        NodeRef.of(functionCall),
                        DereferenceExpression.from(
                                QualifiedName.of(
                                        ImmutableList.<String>builder()
                                                .add(relationshipCteGenerator.getNameMapping().get(functionCall.toString()))
                                                .add(LAMBDA_RESULT_NAME).build())));
            }
            return null;
        }

        private boolean isArrayFunction(QualifiedName funcName)
        {
            // TODO: define what's array function
            //  Refer to trino array function temporarily
            // TODO: bigquery array function mapping
            return List.of("cardinality", "array_max", "array_min", "array_length").contains(funcName.toString());
        }

        private boolean isLambdaFunction(QualifiedName funcName)
        {
            return List.of("transform", "filter").contains(funcName.getSuffix());
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            registerRelationshipCTEs(node)
                    .ifPresent(info -> {
                        // TODO: remove this check after we support directly select relationship column
                        if (node != info.getOriginal()) {
                            relationshipCTENames.add(String.join(".", info.getReplacementNameParts()));
                            relationshipFieldsRewrite.put(NodeRef.of(info.getOriginal()), info.getReplacement());
                        }
                    });
            return null;
        }

        // register needed relationship CTEs and return node replacement information
        private Optional<ReplaceNodeInfo> registerRelationshipCTEs(Expression node)
        {
            if (!scope.isTableScope()) {
                return Optional.empty();
            }

            LinkedList<Expression> elements = elements(node);
            if (elements.isEmpty()) {
                return Optional.empty();
            }

            String baseModelName;
            Expression root = elements.peekFirst();
            LinkedList<String> nameParts = new LinkedList<>();
            LinkedList<RelationshipField> chain = new LinkedList<>();
            // process the root node, root node should be either FunctionCall or Identifier, if not, relationship rewrite won't be fired
            if (root instanceof FunctionCall) {
                FunctionCallProcessor functionCallProcessor = new FunctionCallProcessor();
                FunctionCallProcessorContext functionCallProcessorContext = functionCallProcessor.process(root, null);
                List<RelationshipField> relationshipFields = functionCallProcessorContext.getRelationshipField();
                checkArgument(relationshipFields.size() == 1, "There should be only one relationship field function chain in dereference expression");

                for (FunctionCall functionCall : functionCallProcessor.getReplaceFunctionCalls()) {
                    relationshipFieldsRewrite.put(
                            NodeRef.of(functionCall),
                            DereferenceExpression.from(
                                    QualifiedName.of(
                                            ImmutableList.<String>builder()
                                                    .add(relationshipCteGenerator.getNameMapping().get(functionCall.toString()))
                                                    .add(LAMBDA_RESULT_NAME).build())));
                }
                nameParts.add(elements.pop().toString());
                baseModelName = relationshipFields.get(0).getBaseModelName();
            }
            else if (root instanceof Identifier) {
                List<Field> modelFields = scope.getRelationType()
                        .orElseThrow(() -> new IllegalArgumentException("relation type is empty"))
                        .getFields();

                Optional<Field> relationshipField = Optional.empty();
                // process column with prefix. i.e. [TableAlias|TableName].column
                while (elements.size() > 0 && relationshipField.isEmpty()) {
                    QualifiedName current;
                    Expression element = elements.pop();
                    if (element instanceof Identifier) {
                        current = QualifiedName.of(List.of((Identifier) element));
                    }
                    else if (element instanceof DereferenceExpression) {
                        current = DereferenceExpression.getQualifiedName((DereferenceExpression) element);
                    }
                    else {
                        break;
                    }

                    relationshipField = modelFields.stream()
                            .filter(scopeField -> scopeField.canResolve(current))
                            .filter(Field::isRelationship)
                            .findAny();
                }

                if (relationshipField.isPresent()) {
                    String fieldModelName = relationshipField.get().getModelName().getSchemaTableName().getTableName();
                    String fieldTypeName = relationshipField.get().getType();
                    String fieldName = relationshipField.get().getColumnName();
                    Relationship relationship = graphMDL.getRelationship(relationshipField.get().getRelationship().orElseThrow()).orElseThrow();
                    List<String> parts = List.of(fieldModelName, relationshipField.get().getColumnName());
                    relationships.add(relationship);
                    relationshipCteGenerator.register(
                            parts,
                            access(List.of(rsItem(relationship.getName(), relationship.getModels().get(0).equals(fieldTypeName) ? REVERSE_RS : RS))));

                    baseModelName = relationshipField.get().getModelName().getSchemaTableName().getTableName();
                    nameParts.addAll(parts);
                    chain.add(new RelationshipField(nameParts, fieldModelName, fieldName, relationship, baseModelName));
                }
                else {
                    return Optional.empty();
                }
            }
            else {
                return Optional.empty();
            }

            while (elements.size() > 0) {
                Expression expression = elements.pop();
                if (expression instanceof DereferenceExpression) {
                    DereferenceExpression dereferenceExpression = (DereferenceExpression) expression;
                    RelationshipCTE cte = relationshipCteGenerator.getRelationshipCTEs().get(String.join(".", nameParts));
                    checkArgument(cte != null, String.join(".", nameParts) + " cte not found");

                    Identifier field = dereferenceExpression.getField().orElseThrow();
                    String modelName = cte.getTarget().getName();
                    Optional<Column> relationshipColumn = graphMDL.getModel(modelName)
                            .stream()
                            .map(Model::getColumns)
                            .flatMap(List::stream)
                            .filter(column -> column.getName().equals(field.toString()) && column.getRelationship().isPresent())
                            .findAny();

                    if (relationshipColumn.isPresent()) {
                        Relationship relationship = graphMDL.getRelationship(relationshipColumn.get().getRelationship().get())
                                .orElseThrow(() -> new IllegalArgumentException("Relationship not found"));
                        relationships.add(relationship);
                        String relationshipColumnType = relationshipColumn.get().getType();
                        nameParts.add(field.toString());
                        relationshipCteGenerator.register(
                                nameParts,
                                access(List.of(
                                        rsItem(String.join(".", nameParts.subList(0, nameParts.size() - 1)), CTE),
                                        rsItem(relationship.getName(), relationship.getModels().get(0).equals(relationshipColumnType) ? REVERSE_RS : RS))),
                                baseModelName);
                        chain.add(new RelationshipField(nameParts, modelName, relationshipColumn.get().getName(), relationship, baseModelName));
                    }
                    else {
                        return Optional.of(
                                new ReplaceNodeInfo(
                                        nameParts,
                                        dereferenceExpression.getBase(),
                                        new Identifier(
                                                relationshipCteGenerator.getNameMapping().get(String.join(".", nameParts))),
                                        chain.isEmpty() ? Optional.empty() : Optional.of(chain.getLast())));
                    }
                }
                else if (expression instanceof SubscriptExpression) {
                    SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
                    String index = subscriptExpression.getIndex().toString();
                    String cteName = String.join(".", nameParts);
                    RelationshipCTE cte = relationshipCteGenerator.getRelationshipCTEs().get(cteName);
                    if (cte == null) {
                        return Optional.empty();
                    }
                    Relationship relationship = cte.getRelationship();
                    relationships.add(relationship);
                    String lastNamePart = nameParts.removeLast();
                    nameParts.add(format("%s[%s]", lastNamePart, index));

                    relationshipCteGenerator.register(
                            nameParts,
                            access(List.of(
                                    rsItem(cteName, CTE, index),
                                    rsItem(relationship.getName(), relationship.isReverse() ? REVERSE_RS : RS))),
                            baseModelName,
                            subscriptExpression.getBase() instanceof FunctionCall ? LAMBDA_RESULT_NAME : lastNamePart);
                }
                else {
                    throw new IllegalArgumentException("Unsupported operation");
                }
            }

            return Optional.ofNullable(relationshipCteGenerator.getNameMapping().get(String.join(".", nameParts)))
                    .map(cteName ->
                            new ReplaceNodeInfo(
                                    nameParts,
                                    node,
                                    new Identifier(cteName),
                                    chain.isEmpty() ? Optional.empty() : Optional.of(chain.getLast())));
        }

        private void collectRelationshipLambdaExpression(
                FunctionCall functionCall,
                LambdaExpression lambdaExpression,
                RelationshipField relationshipField,
                Optional<FunctionCall> previousLambdaCall)
        {
            checkArgument(lambdaExpression.getArguments().size() == 1, "lambda expression must have one argument");
            String modelName = relationshipField.getModelName();
            Expression expression = LambdaExpressionBodyRewrite.rewrite(lambdaExpression.getBody(), modelName, lambdaExpression.getArguments().get(0).getName());
            String columnName = relationshipField.getColumnName();
            Relationship relationship = relationshipField.getRelationship();

            RelationshipCteGenerator.RelationshipOperation operation;
            String functionName = functionCall.getName().toString();
            String cteName = previousLambdaCall.map(Expression::toString).orElse(String.join(".", relationshipField.getCteNameParts()));
            Expression unnestField = previousLambdaCall.isPresent() ? DereferenceExpression.from(QualifiedName.of(SOURCE_REFERENCE, LAMBDA_RESULT_NAME)) : null;
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
            else {
                throw new IllegalArgumentException(functionName + " not supported");
            }

            relationshipCteGenerator.register(List.of(functionCall.toString()), operation, relationshipField.getBaseModelName());
        }

        private class FunctionCallProcessorContext
        {
            private final List<RelationshipField> relationshipField;

            public FunctionCallProcessorContext(List<RelationshipField> relationshipField)
            {
                this.relationshipField = requireNonNull(relationshipField, "relationshipField is null");
            }

            public List<RelationshipField> getRelationshipField()
            {
                return relationshipField;
            }
        }

        private class FunctionCallProcessor
                extends AstVisitor<FunctionCallProcessorContext, Void>
        {
            // record lambda function call that doesn't use any lambda function calls
            // e.g. filter(col1, col -> true) will be recorded while filter(filter(col1, col -> true), col -> false) won't
            private final Stack<FunctionCall> rootLambdaCalls = new Stack<>();
            private final Stack<FunctionCall> nodesToReplace = new Stack<>();

            @Override
            protected FunctionCallProcessorContext visitFunctionCall(FunctionCall node, Void ignored)
            {
                List<FunctionCallProcessorContext> contexts = node.getArguments().stream()
                        .filter(argument -> argument instanceof FunctionCall)
                        .map(functionCall -> visitFunctionCall((FunctionCall) functionCall, ignored))
                        .collect(toImmutableList());

                if (isLambdaFunction(node.getName())) {
                    checkArgument(node.getArguments().size() == 2, "Lambda function should have 2 arguments");
                    // TODO: remove this check
                    checkArgument(
                            contexts.stream()
                                    .map(FunctionCallProcessorContext::getRelationshipField)
                                    .flatMap(List::stream)
                                    .map(RelationshipField::getRelationship)
                                    .filter(Objects::nonNull)
                                    .distinct()
                                    .count() <= 1,
                            "Lambda function chain only allow one relationship");

                    Optional<ReplaceNodeInfo> replaceNodeInfo = registerRelationshipCTEs(node.getArguments().get(0));
                    if (replaceNodeInfo.isPresent()
                            && replaceNodeInfo.get().getLastRelationshipField().isPresent()
                            && replaceNodeInfo.get().getOriginal() == node.getArguments().get(0)) {
                        collectRelationshipLambdaExpression(
                                node,
                                (LambdaExpression) node.getArguments().get(1),
                                replaceNodeInfo.get().getLastRelationshipField().get(),
                                Optional.empty());
                        rootLambdaCalls.push(node);
                    }
                    else {
                        nodesToReplace.clear();
                        collectRelationshipLambdaExpression(
                                node,
                                (LambdaExpression) node.getArguments().get(1),
                                contexts.get(0).getRelationshipField().get(0),
                                Optional.of(rootLambdaCalls.pop()));
                        // TODO: remove this check
                        checkArgument(rootLambdaCalls.empty(), "Currently the first argument of a lambda function cannot contain more than one lambda function.");
                    }
                    nodesToReplace.push(node);

                    RelationshipField relationshipField = replaceNodeInfo
                            .flatMap(ReplaceNodeInfo::getLastRelationshipField)
                            .orElseGet(() -> contexts.get(0).getRelationshipField().get(0));

                    return new FunctionCallProcessorContext(List.of(relationshipField));
                }
                else {
                    List<RelationshipField> relationshipFields = new ArrayList<>();
                    for (int i = 0; i < node.getArguments().size(); i++) {
                        Expression argument = node.getArguments().get(i);
                        if (relationshipCteGenerator.getNameMapping().containsKey(argument.toString())) {
                            relationshipFields.addAll(contexts.get(i).getRelationshipField());
                        }
                        else {
                            registerRelationshipCTEs(argument)
                                    .ifPresent(info -> {
                                        info.getLastRelationshipField().ifPresent(relationshipFields::add);
                                        List<String> cteNameParts = info.getReplacementNameParts();
                                        if (isArrayFunction(node.getName())) {
                                            relationshipCTENames.add(String.join(".", cteNameParts));
                                            relationshipFieldsRewrite.put(
                                                    NodeRef.of(info.getOriginal()),
                                                    DereferenceExpression.from(
                                                            QualifiedName.of(info.getReplacement().toString(), cteNameParts.get(cteNameParts.size() - 1))));
                                        }
                                    });
                        }
                    }
                    return new FunctionCallProcessorContext(relationshipFields);
                }
            }

            public List<FunctionCall> getReplaceFunctionCalls()
            {
                return ImmutableList.copyOf(nodesToReplace.iterator());
            }
        }
    }

    private static LinkedList<Expression> elements(Expression expression)
    {
        Expression current = expression;
        LinkedList<Expression> elements = new LinkedList<>();
        while (true) {
            if (current instanceof FunctionCall || current instanceof Identifier) {
                elements.add(current);
                // in dereference expression, function call or identifier should be the root node
                break;
            }
            else if (current instanceof DereferenceExpression) {
                elements.add(current);
                current = ((DereferenceExpression) current).getBase();
            }
            else if (current instanceof SubscriptExpression) {
                elements.add(current);
                current = ((SubscriptExpression) current).getBase();
            }
            else {
                // unexpected node in dereference expression, clear everything and return
                elements.clear();
                break;
            }
        }
        return new LinkedList<>(Lists.reverse(elements));
    }

    private static class RelationshipField
    {
        private final List<String> cteNameParts;
        private final String modelName;
        private final String columnName;
        private final Relationship relationship;
        private final String baseModelName;

        public RelationshipField(List<String> cteNameParts, String modelName, String columnName, Relationship relationship, String baseModelName)
        {
            this.cteNameParts = requireNonNull(cteNameParts);
            this.modelName = requireNonNull(modelName);
            this.columnName = requireNonNull(columnName);
            this.relationship = requireNonNull(relationship);
            this.baseModelName = requireNonNull(baseModelName);
        }

        private List<String> getCteNameParts()
        {
            return cteNameParts;
        }

        public String getModelName()
        {
            return modelName;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Relationship getRelationship()
        {
            return relationship;
        }

        public String getBaseModelName()
        {
            return baseModelName;
        }
    }

    private static class ReplaceNodeInfo
    {
        private final List<String> replacementNameParts;
        private final Expression original;
        private final Expression replacement;
        // TODO: this is required for function call processor, find other better way to search relationship field in function call
        private final Optional<RelationshipField> lastRelationshipField;

        public ReplaceNodeInfo(
                List<String> replacementNameParts,
                Expression original,
                Expression replacement,
                Optional<RelationshipField> lastRelationshipField)
        {
            this.replacementNameParts = requireNonNull(replacementNameParts);
            this.original = requireNonNull(original);
            this.replacement = requireNonNull(replacement);
            this.lastRelationshipField = requireNonNull(lastRelationshipField);
        }

        public List<String> getReplacementNameParts()
        {
            return replacementNameParts;
        }

        public Expression getOriginal()
        {
            return original;
        }

        public Expression getReplacement()
        {
            return replacement;
        }

        public Optional<RelationshipField> getLastRelationshipField()
        {
            return lastRelationshipField;
        }
    }
}
