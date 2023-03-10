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
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.graphmdl.base.Utils.checkArgument;
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

            QualifiedName qualifiedName = getQualifiedName(expression);

            if (qualifiedName == null) {
                return;
            }

            List<Field> scopeFields = scope.getRelationType()
                    .orElseThrow(() -> new IllegalArgumentException("relation type is empty"))
                    .getFields();

            // TODO: we need to support alias here
            // e.g. select a.relationship.column from table a
            int index = 0;
            Optional<Field> optField = Optional.empty();
            for (int i = 0; i < qualifiedName.getParts().size(); i++) {
                QualifiedName partName = QualifiedName.of(qualifiedName.getParts().subList(0, i + 1));
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
            List<String> relNameParts = new ArrayList<>();
            relNameParts.add(modelName);
            for (; index < qualifiedName.getParts().size(); index++) {
                checkArgument(graphMDL.getModel(modelName).isPresent(), modelName + " model not found");
                String partName = qualifiedName.getParts().get(index);
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

                    relNameParts.add(partName);
                    String relNameStr = String.join(".", relNameParts);
                    relationshipCTENames.add(relNameStr);
                    if (!relationshipCteGenerator.getNameMapping().containsKey(relNameStr)) {
                        if (relNameParts.size() == 2) {
                            relationshipCteGenerator.register(
                                    relNameParts,
                                    List.of(RelationshipCteGenerator.RsItem.rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RelationshipCteGenerator.RsItem.Type.REVERSE_RS : RelationshipCteGenerator.RsItem.Type.RS)));
                        }
                        else {
                            relationshipCteGenerator.register(
                                    relNameParts,
                                    List.of(
                                            RelationshipCteGenerator.RsItem.rsItem(String.join(".", relNameParts.subList(0, relNameParts.size() - 1)), RelationshipCteGenerator.RsItem.Type.CTE),
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

            List<String> remainingParts = qualifiedName.getParts().subList(index, qualifiedName.getParts().size());
            if (relNameParts.size() > 1) {
                relationshipFieldsRewrite.put(
                        NodeRef.of(expression),
                        DereferenceExpression.from(
                                QualifiedName.of(
                                        ImmutableList.<String>builder()
                                                .add(relationshipCteGenerator.getNameMapping().get(String.join(".", relNameParts)))
                                                .addAll(remainingParts).build())));
            }
        }

        protected QualifiedName getQualifiedName(Expression expression)
        {
            if (expression instanceof DereferenceExpression) {
                return DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
            }
            if (expression instanceof Identifier) {
                return QualifiedName.of(ImmutableList.of((Identifier) expression));
            }
            return null;
        }
    }
}
