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

package io.wren.base.sqlrewrite.analyzer;

import com.google.common.collect.ImmutableSet;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.wren.base.WrenMDL;
import io.wren.base.dto.Column;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationship;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExpressionRelationshipAnalyzer
{
    private ExpressionRelationshipAnalyzer() {}

    /**
     * Collect to-1 relationships in model field expression, will throw error if to-N relationship(s) exist in expression.
     *
     * @param expression model field expression
     * @param mdl wren mdl
     * @param model the model that expression belongs to
     * @return ExpressionRelationshipInfo
     */
    public static Set<ExpressionRelationshipInfo> getToOneRelationships(Expression expression, WrenMDL mdl, Model model)
    {
        RelationshipCollector collector = new RelationshipCollector(mdl, model, false);
        collector.process(expression);
        return collector.getExpressionRelationshipInfo();
    }

    /**
     * Collect relationships (to-1 and to-N) in model field expression.
     *
     * @param expression model field expression
     * @param mdl wren mdl
     * @param model the model that expression belongs to
     * @return ExpressionRelationshipInfo
     */
    public static Set<ExpressionRelationshipInfo> getRelationships(Expression expression, WrenMDL mdl, Model model)
    {
        RelationshipCollector collector = new RelationshipCollector(mdl, model, true);
        collector.process(expression);
        return collector.getExpressionRelationshipInfo();
    }

    private static class RelationshipCollector
            extends DefaultTraversalVisitor<Void>
    {
        private final WrenMDL wrenMDL;
        private final Model model;
        private final boolean allowToManyRelationship;
        private final Set<ExpressionRelationshipInfo> relationships = new HashSet<>();

        public RelationshipCollector(WrenMDL wrenMDL, Model model, boolean allowToManyRelationship)
        {
            this.wrenMDL = requireNonNull(wrenMDL);
            this.model = requireNonNull(model);
            this.allowToManyRelationship = allowToManyRelationship;
        }

        public Set<ExpressionRelationshipInfo> getExpressionRelationshipInfo()
        {
            return ImmutableSet.copyOf(relationships);
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            if (node.getField().isPresent()) {
                QualifiedName qualifiedName = getQualifiedName(node);
                if (qualifiedName != null) {
                    Optional<ExpressionRelationshipInfo> expressionRelationshipInfo = createRelationshipInfo(qualifiedName, model, wrenMDL);
                    if (expressionRelationshipInfo.isPresent()) {
                        if (!allowToManyRelationship) {
                            validateToOne(expressionRelationshipInfo.get());
                        }
                        relationships.add(expressionRelationshipInfo.get());
                    }
                }
            }
            return null;
        }
    }

    public static Optional<ExpressionRelationshipInfo> createRelationshipInfo(QualifiedName qualifiedName, Model model, WrenMDL mdl)
    {
        List<RelationshipColumnInfo> relationshipColumnInfos = new ArrayList<>();
        Model current = model;
        Relationship baseModelRelationship = null;

        for (int i = 0; i < qualifiedName.getParts().size(); i++) {
            String columnName = qualifiedName.getParts().get(i);
            Optional<Column> relationshipColumnOpt = WrenMDL.getRelationshipColumn(current, columnName);

            if (relationshipColumnOpt.isEmpty()) {
                if (i == 0) {
                    return Optional.empty();
                }
                return buildExpressionRelationshipInfo(qualifiedName, relationshipColumnInfos, baseModelRelationship, i);
            }

            Column relationshipColumn = relationshipColumnOpt.get();
            Relationship relationship = getRelationshipFromMDL(relationshipColumn, mdl);
            relationshipColumnInfos.add(new RelationshipColumnInfo(current, relationshipColumn, relationship));
            if (current == model) {
                baseModelRelationship = relationship;
            }

            current = getNextModel(relationshipColumn, mdl);
            checkForCycle(current, model);
        }

        return Optional.empty();
    }

    private static Relationship getRelationshipFromMDL(Column relationshipColumn, WrenMDL mdl)
    {
        String relationshipName = relationshipColumn.getRelationship().get();
        return mdl.getRelationship(relationshipName)
                .orElseThrow(() -> new NoSuchElementException(format("relationship %s not found", relationshipName)));
    }

    private static Model getNextModel(Column relationshipColumn, WrenMDL mdl)
    {
        return mdl.getModel(relationshipColumn.getType())
                .orElseThrow(() -> new NoSuchElementException(format("model %s not found", relationshipColumn.getType())));
    }

    private static void checkForCycle(Model current, Model model)
    {
        checkArgument(current != model, "found cycle in expression");
    }

    private static Optional<ExpressionRelationshipInfo> buildExpressionRelationshipInfo(
            QualifiedName qualifiedName,
            List<RelationshipColumnInfo> relationshipColumnInfos,
            Relationship baseModelRelationship,
            int index)
    {
        return Optional.of(new ExpressionRelationshipInfo(
                qualifiedName,
                qualifiedName.getParts().subList(0, index),
                qualifiedName.getParts().subList(index, qualifiedName.getParts().size()),
                relationshipColumnInfos,
                baseModelRelationship));
    }

    private static void validateToOne(ExpressionRelationshipInfo expressionRelationshipInfo)
    {
        for (Relationship relationship : expressionRelationshipInfo.getRelationships()) {
            checkArgument(relationship.getJoinType().isToOne(), "expr in model only accept to-one relation");
        }
    }
}
