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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.dto.Column;
import io.graphmdl.base.dto.Relationship;
import io.graphmdl.sqlrewrite.LambdaExpressionBodyRewrite;
import io.graphmdl.sqlrewrite.RelationshipCteGenerator;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SubscriptExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import static io.graphmdl.sqlrewrite.Utils.getNextPart;
import static io.graphmdl.sqlrewrite.analyzer.ExpressionAnalyzer.DereferenceName.dereferenceName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

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
            return ignored;
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
            collectRelationshipFields(node, false, false);
            return ignored;
        }

        // This function only retrieve relationship fields in first layer of expression, and won't recursive traverse inside the expression.
        private Optional<Field> collectRelationshipFields(Expression expression, boolean fromArrayFunctionCall, boolean fromLambdaFunctionCall)
        {
            // we only collect select items in table scope
            if (!scope.isTableScope()) {
                return Optional.empty();
            }

            List<DereferenceName> dereferenceNames = Lists.reverse(toDereferenceNames(expression));
            if (dereferenceNames.isEmpty()) {
                return Optional.empty();
            }

            List<Field> scopeFields = scope.getRelationType()
                    .orElseThrow(() -> new IllegalArgumentException("relation type is empty"))
                    .getFields();

            int index = 0;
            Optional<Field> optField = Optional.empty();
            for (int i = 0; i < dereferenceNames.size(); i++) {
                QualifiedName partName = QualifiedName.of(dereferenceNames.subList(0, i + 1).stream().map(DereferenceName::getIdentifier).collect(toList()));
                optField = scopeFields.stream().filter(scopeField -> scopeField.canResolve(partName)).findAny();
                if (optField.isPresent() && optField.get().isRelationship()) {
                    index = partName.getParts().size() - 1;
                    break;
                }
            }

            // means there is no matched field
            if (optField.isEmpty()) {
                return Optional.empty();
            }

            String modelName = optField.get().getModelName().getSchemaTableName().getTableName();
            List<DereferenceName> relNameParts = new ArrayList<>();
            relNameParts.add(dereferenceName(modelName));
            for (; index < dereferenceNames.size(); index++) {
                checkArgument(graphMDL.getModel(modelName).isPresent(), modelName + " model not found");
                String partName = dereferenceNames.get(index).getIdentifier().getValue();
                Optional<Column> columnOptional = graphMDL.getModel(modelName).get().getColumns().stream()
                        .filter(col -> col.getName().equals(partName))
                        .findAny();

                if (columnOptional.isEmpty()) {
                    continue;
                }

                Column column = columnOptional.get();

                if (column.getRelationship().isPresent()) {
                    // if column is a relationship, it's type name is model name
                    modelName = column.getType();
                    Relationship relationship = graphMDL.getRelationship(column.getRelationship().get())
                            .orElseThrow(() -> new IllegalArgumentException(column.getRelationship().get() + " relationship not found"));
                    checkArgument(relationship.getModels().contains(modelName), format("relationship %s doesn't contain model %s", relationship.getName(), modelName));
                    relationships.add(relationship);

                    relNameParts.add(dereferenceNames.get(index));
                    String relNameStr = relNameParts.stream().map(DereferenceName::toString).collect(joining("."));
                    // If a collection is called from lambda function, it means this relationship is used in a lambda function CTE.
                    // We don't need to join it in the main query.
                    if (!fromLambdaFunctionCall) {
                        relationshipCTENames.add(relNameStr);
                    }

                    if (!relationshipCteGenerator.getNameMapping().containsKey(relNameStr)) {
                        if (relNameParts.size() == 2) {
                            relationshipCteGenerator.register(
                                    getBaseParts(relNameParts),
                                    access(List.of(rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? REVERSE_RS : RS))));
                        }
                        else {
                            relationshipCteGenerator.register(
                                    getBaseParts(relNameParts),
                                    access(List.of(
                                            rsItem(String.join(".", relNameParts.stream().map(DereferenceName::toString).collect(toList()).subList(0, relNameParts.size() - 1)), CTE),
                                            rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? REVERSE_RS : RS))));
                        }

                        if (dereferenceNames.get(index).getIndex().isPresent()) {
                            List<String> indexParts = ImmutableList.<String>builder().addAll(relNameParts.stream().map(DereferenceName::toString).collect(toList())).build();
                            relationshipCteGenerator.register(
                                    indexParts,
                                    access(List.of(
                                            rsItem(String.join(".", getBaseParts(relNameParts)), CTE, dereferenceNames.get(index).getIndex().get().toString()),
                                            rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? REVERSE_RS : RS))));
                        }
                    }
                }
                else {
                    break;
                }
            }

            // An array function invoking is allowed to access a one-to-many relationship fields.
            if (fromArrayFunctionCall) {
                index = index - 1;
            }

            List<String> remainingParts = dereferenceNames.subList(index, dereferenceNames.size()).stream().map(DereferenceName::getIdentifier).map(Identifier::getValue).collect(toList());
            if (relNameParts.size() > 1 && remainingParts.size() > 0) {
                relationshipFieldsRewrite.put(
                        NodeRef.of(expression),
                        DereferenceExpression.from(
                                QualifiedName.of(
                                        ImmutableList.<String>builder()
                                                .add(relationshipCteGenerator.getNameMapping().get(relNameParts.stream().map(DereferenceName::toString).collect(joining("."))))
                                                .addAll(remainingParts).build())));
            }
            return optField;
        }

        private void collectRelationshipLambdaExpression(
                FunctionCall functionCall,
                LambdaExpression lambdaExpression,
                Field baseField,
                Optional<FunctionCall> previousLambdaCall)
        {
            checkArgument(baseField.isRelationship(), "base field must be a relationship");
            checkArgument(lambdaExpression.getArguments().size() == 1, "lambda expression must have one argument");
            Expression expression = LambdaExpressionBodyRewrite.rewrite(lambdaExpression.getBody(), baseField, lambdaExpression.getArguments().get(0).getName());
            String modelName = baseField.getModelName().getSchemaTableName().getTableName();
            QualifiedName baseName = QualifiedName.of(modelName, baseField.getName().get());
            String relationshipName = baseField.getRelationship().get();
            Relationship relationship = graphMDL.getRelationship(relationshipName)
                    .orElseThrow(() -> new IllegalArgumentException(relationshipName + " relationship not found"));

            RelationshipCteGenerator.RelationshipOperation operation;
            String functionName = functionCall.getName().toString();
            String cteName = previousLambdaCall.map(Expression::toString).orElse(baseName.toString());
            Expression unnestField = previousLambdaCall.isPresent() ? DereferenceExpression.from(QualifiedName.of(SOURCE_REFERENCE, LAMBDA_RESULT_NAME)) : null;
            if (functionName.equalsIgnoreCase("transform")) {
                operation = transform(
                        List.of(rsItem(cteName, CTE),
                                rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RS : REVERSE_RS)),
                        expression,
                        baseField.getColumnName(),
                        unnestField);
            }
            else if (functionName.equalsIgnoreCase("filter")) {
                operation = filter(
                        List.of(rsItem(cteName, CTE),
                                rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RS : REVERSE_RS)),
                        expression,
                        baseField.getColumnName(),
                        unnestField);
            }
            else {
                throw new IllegalArgumentException(functionName + " not supported");
            }

            relationshipCteGenerator.register(List.of(functionCall.toString()), operation, modelName);
        }

        private class FunctionCallProcessorContext
        {
            private final List<Field> relationshipField;

            public FunctionCallProcessorContext(List<Field> relationshipField)
            {
                this.relationshipField = requireNonNull(relationshipField, "relationshipField is null");
            }

            public List<Field> getRelationshipField()
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
                                    .map(rsField -> rsField.getRelationship().orElse(null))
                                    .filter(Objects::nonNull)
                                    .distinct()
                                    .count() <= 1,
                            "Lambda function chain only allow one relationship");

                    // if exists, means first argument is a field
                    Optional<Field> field = collectRelationshipFields(node.getArguments().get(0), true, true);
                    if (field.isPresent()) {
                        collectRelationshipLambdaExpression(
                                node,
                                (LambdaExpression) node.getArguments().get(1),
                                field.get(),
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

                    Field relationshipField = field.orElseGet(() -> contexts.get(0).getRelationshipField().get(0));
                    return new FunctionCallProcessorContext(List.of(relationshipField));
                }
                else {
                    List<Field> relationshipFields = new ArrayList<>();
                    for (int i = 0; i < node.getArguments().size(); i++) {
                        Expression argument = node.getArguments().get(i);
                        if (relationshipCteGenerator.getNameMapping().containsKey(argument.toString())) {
                            relationshipFields.addAll(contexts.get(i).getRelationshipField());
                        }
                        else {
                            collectRelationshipFields(argument, isArrayFunction(node.getName()), false)
                                    .ifPresent(relationshipFields::add);
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

    private List<String> getBaseParts(List<DereferenceName> dereferenceNames)
    {
        ImmutableList.Builder<String> baseParts = ImmutableList.<String>builder()
                .addAll(dereferenceNames
                        .subList(0, dereferenceNames.size() - 1).stream()
                        .map(DereferenceName::toString)
                        .collect(toList()));
        baseParts.add(Iterables.getLast(dereferenceNames).getIdentifier().getValue());
        return baseParts.build();
    }

    static List<DereferenceName> toDereferenceNames(Expression expression)
    {
        ImmutableList.Builder<DereferenceName> builder = ImmutableList.builder();
        while (expression instanceof DereferenceExpression || expression instanceof SubscriptExpression) {
            builder.add(toDereferenceName(expression));
            if (expression instanceof DereferenceExpression) {
                expression = ((DereferenceExpression) expression).getBase();
            }
            else {
                SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
                expression = getNextPart(subscriptExpression);
                if (expression.equals(subscriptExpression.getBase())) {
                    // If the next part is same as its base, it means it's the last part.
                    return builder.build();
                }
            }
        }

        if (expression instanceof Identifier) {
            builder.add(toDereferenceName(expression));
        }

        return builder.build();
    }

    private static DereferenceName toDereferenceName(Expression expression)
    {
        if (expression instanceof DereferenceExpression || expression instanceof Identifier) {
            return new DereferenceName(getField(expression));
        }
        else if (expression instanceof SubscriptExpression) {
            SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
            return new DereferenceName(getField(subscriptExpression.getBase()), subscriptExpression.getIndex());
        }
        throw new IllegalArgumentException("Unsupported expression type: " + expression.getClass().getName());
    }

    private static Identifier getField(Expression expression)
    {
        if (expression instanceof DereferenceExpression) {
            return ((DereferenceExpression) expression).getField().orElseThrow();
        }
        else if (expression instanceof Identifier) {
            return (Identifier) expression;
        }
        return null;
    }

    static class DereferenceName
    {
        public static DereferenceName dereferenceName(String name)
        {
            return new DereferenceName(new Identifier(name));
        }

        public static DereferenceName dereferenceName(String name, int index)
        {
            return new DereferenceName(new Identifier(name), new LongLiteral(String.valueOf(index)));
        }

        private final Identifier identifier;
        private final Expression index;

        private DereferenceName(Identifier identifier)
        {
            this(identifier, null);
        }

        private DereferenceName(Identifier identifier, Expression index)
        {
            this.identifier = identifier;
            this.index = index;
        }

        public Identifier getIdentifier()
        {
            return identifier;
        }

        public Optional<Expression> getIndex()
        {
            return Optional.ofNullable(index);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            DereferenceName that = (DereferenceName) obj;
            return Objects.equals(identifier, that.identifier) &&
                    Objects.equals(index, that.index);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identifier, index);
        }

        @Override
        public String toString()
        {
            if (index != null) {
                return identifier + "[" + index + "]";
            }
            return identifier.toString();
        }
    }
}
