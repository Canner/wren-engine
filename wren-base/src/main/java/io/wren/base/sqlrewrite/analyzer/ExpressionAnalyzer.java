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

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowOperation;
import io.trino.sql.tree.WindowSpecification;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ExpressionAnalyzer
{
    private ExpressionAnalyzer() {}

    public static ExpressionAnalysis analyze(Scope scope, Expression expression, SessionContext sessionContext, WrenMDL wrenMDL, Analysis analysis)
    {
        ExpressionVisitor visitor = new ExpressionVisitor(scope, sessionContext, wrenMDL, analysis);
        visitor.process(expression);

        return new ExpressionAnalysis(visitor.getReferenceFields(), visitor.getPredicates(), visitor.isRequireRelation());
    }

    private static class ExpressionVisitor
            extends DefaultTraversalVisitor<Void>
    {
        private final Scope scope;
        private final WrenMDL wrenMDL;
        private final SessionContext sessionContext;
        private final Analysis analysis;
        private final Map<NodeRef<Expression>, Field> referenceFields = new HashMap<>();
        private final List<ComparisonExpression> predicates = new ArrayList<>();
        private boolean requireRelation;

        public ExpressionVisitor(
                Scope scope,
                SessionContext sessionContext,
                WrenMDL wrenMDL,
                Analysis analysis)
        {
            this.scope = requireNonNull(scope, "scope is null");
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.wrenMDL = requireNonNull(wrenMDL, "wrenMDL is null");
            this.analysis = requireNonNull(analysis, "analysis is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            QualifiedName qualifiedName = getQualifiedName(node);
            if (qualifiedName != null) {
                scope.resolveAnyField(qualifiedName)
                        .ifPresent(field -> referenceFields.put(NodeRef.of(node), field));
            }
            else {
                // The base could be SubscriptExpression, so we need to process it
                process(node.getBase());
            }

            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void context)
        {
            QualifiedName qualifiedName = QualifiedName.of(ImmutableList.of(node));
            scope.resolveAnyField(qualifiedName)
                    .ifPresent(field -> referenceFields.put(NodeRef.of(node), field));
            return null;
        }

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            QualifiedName qualifiedName = getQualifiedName(node.getBase());
            scope.resolveAnyField(qualifiedName)
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
            node.getWindow().ifPresent(this::analyzeWindow);
            node.getFilter().ifPresent(this::process);
            node.getOrderBy().ifPresent(orderBy -> orderBy.getSortItems().forEach(sortItem -> process(sortItem.getSortKey())));
            return null;
        }

        @Override
        protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            StatementAnalyzer.analyze(analysis, node.getQuery(), sessionContext, wrenMDL, Optional.of(scope));
            return null;
        }

        @Override
        protected Void visitWindowOperation(WindowOperation node, Void context)
        {
            analyzeWindow(node.getWindow());
            return null;
        }

        private void analyzeWindow(Window window)
        {
            if (window instanceof WindowSpecification windowSpecification) {
                windowSpecification.getPartitionBy().forEach(this::process);
                windowSpecification.getOrderBy().ifPresent(orderBy -> orderBy.getSortItems().forEach(sortItem -> process(sortItem.getSortKey())));
                windowSpecification.getFrame().ifPresent(frame -> {
                    frame.getStart().getValue().ifPresent(this::process);
                    frame.getEnd().flatMap(FrameBound::getValue).ifPresent(this::process);
                });
            }
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

    private static QualifiedName getQualifiedName(Expression expression)
    {
        return switch (expression) {
            case DereferenceExpression dereferenceExpression -> DereferenceExpression.getQualifiedName(dereferenceExpression);
            case Identifier identifier -> QualifiedName.of(ImmutableList.of(identifier));
            default -> null;
        };
    }
}
