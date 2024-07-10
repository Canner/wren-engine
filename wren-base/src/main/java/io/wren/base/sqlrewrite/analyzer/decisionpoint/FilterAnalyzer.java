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

package io.wren.base.sqlrewrite.analyzer.decisionpoint;

import io.trino.sql.ExpressionFormatter;
import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.Node;
import io.wren.base.sqlrewrite.analyzer.Scope;

import java.util.List;

/**
 * Only extract the top level logical expression. If a logical expression is contained by other expression, it will be treated as a single expression.
 */
public class FilterAnalyzer
{
    private FilterAnalyzer() {}

    public static FilterAnalysis analyze(Expression expression, Scope scope)
    {
        return new Visitor(scope).process(expression, null);
    }

    static class Visitor
            extends AstVisitor<FilterAnalysis, Node>
    {
        private final Scope scope;

        private Visitor(Scope scope)
        {
            this.scope = scope;
        }

        @Override
        protected FilterAnalysis visitExpression(Expression node, Node context)
        {
            if (node instanceof LogicalExpression) {
                return process(node, context);
            }
            List<ExprSource> exprSources = List.copyOf(RelationAnalyzer.ExpressionSourceAnalyzer.analyze(node, scope));
            return FilterAnalysis.expression(ExpressionFormatter.formatExpression(node, SqlFormatter.Dialect.DEFAULT), node.getLocation().orElse(null), exprSources);
        }

        @Override
        protected FilterAnalysis visitLogicalExpression(LogicalExpression node, Node parent)
        {
            if (parent == null || parent instanceof LogicalExpression) {
                if (node.getOperator().equals(LogicalExpression.Operator.AND)) {
                    return FilterAnalysis.and(process(node.getChildren().get(0), node), process(node.getChildren().get(1), node), node.getLocation().orElse(null));
                }
                if (node.getOperator().equals(LogicalExpression.Operator.OR)) {
                    return FilterAnalysis.or(process(node.getChildren().get(0), node), process(node.getChildren().get(1), node), node.getLocation().orElse(null));
                }
            }
            List<ExprSource> exprSources = List.copyOf(RelationAnalyzer.ExpressionSourceAnalyzer.analyze(node, scope));
            return FilterAnalysis.expression(ExpressionFormatter.formatExpression(node, SqlFormatter.Dialect.DEFAULT), node.getLocation().orElse(null), exprSources);
        }
    }
}
