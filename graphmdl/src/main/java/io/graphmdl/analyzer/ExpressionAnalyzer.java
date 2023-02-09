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

package io.graphmdl.analyzer;

import com.google.common.collect.ImmutableList;
import io.graphmdl.RelationshipCteGenerator;
import io.graphmdl.base.GraphML;
import io.graphmdl.base.Utils;
import io.graphmdl.base.dto.Column;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.Relationship;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ExpressionAnalyzer
{
    private ExpressionAnalyzer() {}

    private final Map<NodeRef<DereferenceExpression>, DereferenceExpression> relationshipFieldsRewrite = new HashMap<>();
    private final Set<String> relationshipCTENames = new HashSet<>();
    private final Set<Relationship> relationships = new HashSet<>();

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
        return new ExpressionAnalysis(relationshipFieldsRewrite, relationshipCTENames, relationships);
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
            process(node.getLeft());
            process(node.getRight());
            return ignored;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            collectRelationshipFields(node);
            return ignored;
        }

        private void collectRelationshipFields(DereferenceExpression expression)
        {
            // we only collect select items in table scope
            if (!scope.isTableScope()) {
                return;
            }
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(expression);
            if (qualifiedName == null) {
                return;
            }

            List<Field> scopeFields = scope.getRelationType()
                    .orElseThrow(() -> new IllegalArgumentException("relation type is empty"))
                    .getFields();
            String firstName = qualifiedName.getParts().get(0);
            int index = 0;
            boolean isModelName = scopeFields.stream().anyMatch(scopeField -> scopeField.getModelName().equals(firstName));
            if (isModelName) {
                index++;
            }

            String name = qualifiedName.getParts().get(index);
            Optional<Field> optField = scopeFields.stream()
                    .filter(scopeField -> scopeField.getColumnName().equals(name))
                    .findAny();
            // field not found, maybe it's not part of model
            if (optField.isEmpty()) {
                return;
            }
            Field field = optField.get();

            Model model = graphML.getModel(field.getModelName()).orElseThrow(() -> new IllegalArgumentException(field.getModelName() + " model not found"));

            String modelName = model.getName();
            List<String> relNameParts = new ArrayList<>();
            relNameParts.add(modelName);
            for (; index < qualifiedName.getParts().size(); index++) {
                Utils.checkArgument(graphML.getModel(modelName).isPresent(), modelName + " model not found");
                String partName = qualifiedName.getParts().get(index);
                Column column = graphML.getModel(modelName).get().getColumns().stream()
                        .filter(col -> col.getName().equals(partName))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(partName + " column not found"));
                if (column.getRelationship().isPresent()) {
                    // if column is a relationship, it's type name is model name
                    modelName = column.getType();
                    Relationship relationship = graphML.getRelationship(column.getRelationship().get())
                            .orElseThrow(() -> new IllegalArgumentException(column.getRelationship().get() + " relationship not found"));
                    Utils.checkArgument(relationship.getModels().contains(modelName), format("relationship %s doesn't contain model %s", relationship.getName(), modelName));
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

            List<String> remainingParts = qualifiedName.getParts().subList(index, qualifiedName.getParts().size());
            if (relNameParts.size() > 1) {
                relationshipFieldsRewrite.put(
                        NodeRef.of(expression),
                        (DereferenceExpression) DereferenceExpression.from(
                                QualifiedName.of(
                                        ImmutableList.<String>builder()
                                                .add(relationshipCteGenerator.getNameMapping().get(String.join(".", relNameParts)))
                                                .addAll(remainingParts).build())));
            }
        }
    }
}
