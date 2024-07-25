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

import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;

import java.util.Optional;

/**
 * Try to find the most left-side location of an expression.
 * For example, the binary expression "a + b" will return the location of "a".
 * The comparison expression "a = b" will return the location of "a".
 * The location of the expression itself will be returned if it is not a binary or comparison expression.
 */
public class ExpressionLocationAnalyzer
{
    private ExpressionLocationAnalyzer() {}

    public static Optional<NodeLocation> analyze(Node node)
    {
        Visitor visitor = new Visitor();
        visitor.process(node, null);
        return visitor.nodeLocation;
    }

    static class Visitor
            extends DefaultTraversalVisitor<Void>
    {
        private Optional<NodeLocation> nodeLocation = Optional.empty();

        @Override
        protected Void visitExpression(Expression node, Void context)
        {
            nodeLocation = node.getLocation();
            return null;
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void context)
        {
            nodeLocation = node.getLeft().getLocation();
            return null;
        }

        @Override
        protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            nodeLocation = node.getLeft().getLocation();
            return null;
        }
    }
}
