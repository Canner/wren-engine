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

package io.cml.graphml.analyzer;

import com.google.common.collect.ImmutableList;
import io.cml.graphml.RelationshipCteGenerator;
import io.cml.graphml.RelationshipCteGenerator.RsItem;
import io.cml.graphml.base.GraphML;
import io.cml.graphml.base.dto.Column;
import io.cml.graphml.base.dto.Model;
import io.cml.graphml.base.dto.Relationship;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.cml.graphml.RelationshipCteGenerator.RsItem.rsItem;
import static io.cml.graphml.base.Utils.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ExpressionAnalyzer
{
    private ExpressionAnalyzer() {}

    private final Map<NodeRef<DereferenceExpression>, DereferenceExpression> relationshipFieldsRewrite = new HashMap<>();

    public static ExpressionAnalysis analyze(
            Expression expression,
            GraphML graphML,
            RelationshipCteGenerator relationshipCteGenerator,
            Scope scope)
    {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer();
        return expressionAnalyzer.analyzeExpression(expression, graphML, relationshipCteGenerator, scope);
    }

    private ExpressionAnalysis analyzeExpression(
            Expression expression,
            GraphML graphML,
            RelationshipCteGenerator relationshipCteGenerator,
            Scope scope)
    {
        new Visitor(graphML, relationshipCteGenerator, scope).process(expression);
        return new ExpressionAnalysis(relationshipFieldsRewrite);
    }

    private class Visitor
            extends AstVisitor<Void, Void>
    {
        private final GraphML graphML;
        private final RelationshipCteGenerator relationshipCteGenerator;
        private final Scope scope;

        public Visitor(GraphML graphML, RelationshipCteGenerator relationshipCteGenerator, Scope scope)
        {
            this.graphML = requireNonNull(graphML, "graphML is null");
            this.relationshipCteGenerator = requireNonNull(relationshipCteGenerator, "relationshipCteGenerator is null");
            this.scope = requireNonNull(scope, "scope is null");
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void ignored)
        {
            collectRelationshipFields(node.getLeft());
            collectRelationshipFields(node.getLeft());
            return ignored;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            collectRelationshipFields(node);
            return ignored;
        }

        private void collectRelationshipFields(Expression expression)
        {
            // we only collect select items in table scope
            if (!scope.isTableScope()) {
                return;
            }
            if (!(expression instanceof DereferenceExpression)) {
                return;
            }
            DereferenceExpression node = (DereferenceExpression) expression;
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);
            if (qualifiedName == null) {
                return;
            }

            List<Field> scopeFields = scope.getRelationType().get().getFields();
            String firstName = qualifiedName.getParts().get(0);
            int index = 0;
            boolean isModelName = scopeFields.stream().anyMatch(scopeField -> scopeField.getModelName().equals(firstName));
            if (isModelName) {
                index++;
            }

            String name = qualifiedName.getParts().get(index);
            Field field = scopeFields.stream()
                    .filter(scopeField -> scopeField.getColumnName().equals(name))
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException(name + " field not found in scope"));

            Model model = graphML.getModel(field.getModelName()).orElseThrow(() -> new IllegalArgumentException(field.getModelName() + " model not found"));

            String modelName = model.getName();
            List<String> relNameParts = new ArrayList<>();
            relNameParts.add(modelName);
            for (; index < qualifiedName.getParts().size(); index++) {
                String partName = qualifiedName.getParts().get(index);
                Column column = graphML.getModel(modelName).get().getColumns().stream().filter(col -> col.getName().equals(partName)).findAny().get();
                if (column.getRelationship().isPresent()) {
                    // if column is a relationship, it's type name is model name
                    modelName = column.getType();
                    Relationship relationship = graphML.getRelationship(column.getRelationship().get())
                            .orElseThrow(() -> new IllegalArgumentException(column.getRelationship().get() + " relationship not found"));
                    checkArgument(relationship.getModels().contains(modelName), format("relationship %s doesn't contain model %s", relationship.getName(), modelName));

                    relNameParts.add(partName);
                    String relNameStr = String.join(".", relNameParts);
                    if (!relationshipCteGenerator.getNameMapping().containsKey(relNameStr)) {
                        if (relNameParts.size() == 2) {
                            relationshipCteGenerator.register(
                                    relNameStr,
                                    List.of(rsItem(relationship.getName(), RsItem.Type.RS)));
                        }
                        else {
                            relationshipCteGenerator.register(
                                    relNameStr,
                                    List.of(
                                            rsItem(String.join(".", relNameParts.subList(0, relNameParts.size() - 1)), RsItem.Type.CTE),
                                            rsItem(relationship.getName(), relationship.getModels().get(0).equals(modelName) ? RsItem.Type.REVERSE_RS : RsItem.Type.RS)));
                        }
                    }
                }
                else {
                    break;
                }
            }

            List<String> remainingParts = qualifiedName.getParts().subList(index, qualifiedName.getParts().size());
            if (relNameParts.size() > 1) {
                relationshipFieldsRewrite.put(
                        NodeRef.of(node),
                        (DereferenceExpression) DereferenceExpression.from(
                                QualifiedName.of(
                                        ImmutableList.<String>builder()
                                                .add(relationshipCteGenerator.getNameMapping().get(String.join(".", relNameParts)))
                                                .addAll(remainingParts).build())));
            }
        }
    }
}
