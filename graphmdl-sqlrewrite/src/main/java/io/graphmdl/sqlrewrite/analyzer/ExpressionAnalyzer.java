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
import io.graphmdl.sqlrewrite.RelationshipCteGenerator;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
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

import static io.graphmdl.base.Utils.checkArgument;
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
        return new ExpressionAnalysis(relationshipFieldsRewrite, relationshipCTENames, relationships);
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
            if (isArrayFunction(node.getName())) {
                for (Expression argument : node.getArguments()) {
                    collectRelationshipFields(argument, true);
                }
            }
            return ignored;
        }

        private boolean isArrayFunction(QualifiedName funcName)
        {
            // TODO: define what's array function
            return true;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            collectRelationshipFields(node, false);
            return ignored;
        }

        private void collectRelationshipFields(Expression expression, boolean fromArrayFunctionCall)
        {
            // we only collect select items in table scope
            if (!scope.isTableScope()) {
                return;
            }

            List<DereferenceName> dereferenceNames = Lists.reverse(toDereferenceNames(expression));
            if (dereferenceNames.isEmpty()) {
                return;
            }

            List<Field> scopeFields = scope.getRelationType()
                    .orElseThrow(() -> new IllegalArgumentException("relation type is empty"))
                    .getFields();

            // TODO: we need to support alias here
            // e.g. select a.relationship.column from table a
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
                return;
            }

            String modelName = optField.get().getModelName().getSchemaTableName().getTableName();
            List<DereferenceName> relNameParts = new ArrayList<>();
            relNameParts.add(dereferenceName(modelName));
            for (; index < dereferenceNames.size(); index++) {
                checkArgument(graphMDL.getModel(modelName).isPresent(), modelName + " model not found");
                String partName = dereferenceNames.get(index).getIdentifier().getValue();
                // TODO: support colum name with relation prefix
                Column column = graphMDL.getModel(modelName).get().getColumns().stream()
                        .filter(col -> col.getName().equals(partName))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(partName + " column not found"));
                if (column.getRelationship().isPresent()) {
                    // if column is a relationship, it's type name is model name
                    modelName = column.getType();
                    Relationship relationship = graphMDL.getRelationship(column.getRelationship().get())
                            .orElseThrow(() -> new IllegalArgumentException(column.getRelationship().get() + " relationship not found"));
                    checkArgument(relationship.getModels().contains(modelName), format("relationship %s doesn't contain model %s", relationship.getName(), modelName));
                    relationships.add(relationship);

                    relNameParts.add(dereferenceNames.get(index));
                    String relNameStr = String.join(".", relNameParts.stream().map(DereferenceName::toString).collect(toList()));
                    relationshipCTENames.add(relNameStr);

                    if (!relationshipCteGenerator.getNameMapping().containsKey(relNameStr)) {
                        if (relNameParts.size() == 2) {
                            relationshipCteGenerator.register(
                                    getBaseParts(relNameParts),
                                    List.of(RelationshipCteGenerator.RsItem.rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RelationshipCteGenerator.RsItem.Type.REVERSE_RS : RelationshipCteGenerator.RsItem.Type.RS)));
                        }
                        else {
                            relationshipCteGenerator.register(
                                    getBaseParts(relNameParts),
                                    List.of(
                                            RelationshipCteGenerator.RsItem.rsItem(String.join(".", relNameParts.stream().map(DereferenceName::toString).collect(toList()).subList(0, relNameParts.size() - 1)), RelationshipCteGenerator.RsItem.Type.CTE),
                                            RelationshipCteGenerator.RsItem.rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RelationshipCteGenerator.RsItem.Type.REVERSE_RS : RelationshipCteGenerator.RsItem.Type.RS)));
                        }

                        if (dereferenceNames.get(index).getIndex().isPresent()) {
                            List<String> indexParts = ImmutableList.<String>builder().addAll(relNameParts.stream().map(DereferenceName::toString).collect(toList())).build();
                            relationshipCteGenerator.register(
                                    indexParts,
                                    List.of(
                                            RelationshipCteGenerator.RsItem.rsItem(String.join(".", getBaseParts(relNameParts)), RelationshipCteGenerator.RsItem.Type.CTE, dereferenceNames.get(index).getIndex().get().toString()),
                                            RelationshipCteGenerator.RsItem.rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RelationshipCteGenerator.RsItem.Type.REVERSE_RS : RelationshipCteGenerator.RsItem.Type.RS)));
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
            if (relNameParts.size() > 1) {
                relationshipFieldsRewrite.put(
                        NodeRef.of(expression),
                        DereferenceExpression.from(
                                QualifiedName.of(
                                        ImmutableList.<String>builder()
                                                .add(relationshipCteGenerator.getNameMapping().get(relNameParts.stream().map(DereferenceName::toString).collect(joining("."))))
                                                .addAll(remainingParts).build())));
            }
        }
    }

    private List<String> getBaseParts(List<DereferenceName> dereferenceNames)
    {
        ImmutableList.Builder<String> baseParts = ImmutableList.<String>builder().addAll(dereferenceNames.subList(0, dereferenceNames.size() - 1).stream().map(DereferenceName::toString).collect(toList()));
        baseParts.add(Iterables.getLast(dereferenceNames).getIdentifier().getValue());
        return baseParts.build();
    }

    static List<DereferenceName> toDereferenceNames(Expression expression)
    {
        ImmutableList.Builder<DereferenceName> builder = ImmutableList.builder();

        if (expression instanceof Identifier) {
            builder.add(toDereferenceName(expression));
            return builder.build();
        }

        while (expression instanceof DereferenceExpression || expression instanceof SubscriptExpression) {
            DereferenceName dereferenceName = toDereferenceName(expression);
            builder.add(dereferenceName);
            if (expression instanceof DereferenceExpression) {
                expression = ((DereferenceExpression) expression).getBase();
            }
            else {
                Expression base = ((SubscriptExpression) expression).getBase();
                expression = getNextPart((SubscriptExpression) expression);
                if (expression.equals(base)) {
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

    private static Expression getNextPart(SubscriptExpression subscriptExpression)
    {
        Expression base = subscriptExpression.getBase();
        if (base instanceof DereferenceExpression) {
            return ((DereferenceExpression) base).getBase();
        }
        return base;
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
            return ((DereferenceExpression) expression).getField();
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

        public DereferenceName(Identifier identifier)
        {
            this(identifier, null);
        }

        public DereferenceName(Identifier identifier, Expression index)
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
