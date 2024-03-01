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

package io.accio.base.sqlrewrite.analyzer;

import com.google.common.collect.ImmutableList;
import io.accio.base.AccioMDL;
import io.accio.base.type.PGType;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.util.Objects.requireNonNull;

public class ExpressionAnalyzer
{
    private ExpressionAnalyzer() {}

    public static ExpressionAnalysis analyze(AccioMDL mdl, Scope scope, Expression expression)
    {
        ExpressionVisitor visitor = new ExpressionVisitor(scope);
        visitor.process(expression);

        Map<Expression, PGType<?>> expressionTypes = new HashMap<>();
        analyzeExpressionType(mdl, scope, expression, expressionTypes);

        return new ExpressionAnalysis(visitor.getReferenceFields(), visitor.getPredicates(), visitor.isRequireRelation(), expressionTypes);
    }

    private static void analyzeExpressionType(AccioMDL mdl, Scope scope, Expression expression, Map<Expression, PGType<?>> expressionTypes)
    {
        expressionTypes.put(expression, ExpressionTypeAnalyzer.analyze(mdl, scope, expression));
        expression.getChildren().stream()
                .filter(child -> child instanceof Expression)
                .map(child -> (Expression) child)
                .forEach(e -> analyzeExpressionType(mdl, scope, e, expressionTypes));
    }

    private static class ExpressionVisitor
            extends DefaultTraversalVisitor<Void>
    {
        private final Scope scope;
        private final Map<NodeRef<Expression>, Field> referenceFields = new HashMap<>();
        private final List<ComparisonExpression> predicates = new ArrayList<>();
        private boolean requireRelation;

        public ExpressionVisitor(Scope scope)
        {
            this.scope = requireNonNull(scope);
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            QualifiedName qualifiedName = getQualifiedName(node);
            if (qualifiedName != null) {
                scope.getRelationType().getFields().stream()
                        .filter(field -> field.canResolve(qualifiedName))
                        .findAny()
                        .ifPresent(field -> referenceFields.put(NodeRef.of(node), field));
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void context)
        {
            QualifiedName qualifiedName = QualifiedName.of(ImmutableList.of(node));
            scope.getRelationType().getFields().stream()
                    .filter(field -> field.canResolve(qualifiedName))
                    .findAny()
                    .ifPresent(field -> referenceFields.put(NodeRef.of(node), field));
            return null;
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void context)
        {
            process(node.getLeft());
            process(node.getRight());
            predicates.add(node);
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void context)
        {
            if (node.getName().getSuffix().equalsIgnoreCase("count") && node.getArguments().isEmpty()) {
                requireRelation = true;
                return null;
            }
            node.getArguments().forEach(this::process);
            return null;
        }

        public Map<NodeRef<Expression>, Field> getReferenceFields()
        {
            return referenceFields;
        }

        public List<ComparisonExpression> getPredicates()
        {
            return predicates;
        }

        public boolean isRequireRelation()
        {
            return requireRelation;
        }
    }
}
