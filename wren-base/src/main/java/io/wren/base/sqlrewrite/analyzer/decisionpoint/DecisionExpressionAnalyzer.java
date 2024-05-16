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
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;

import java.util.Map;

public class DecisionExpressionAnalyzer
{
    public static final DecisionExpressionAnalysis DEFAULT_ANALYSIS = new DecisionExpressionAnalysis(false, false);
    public static final String INCLUDE_FUNCTION_CALL = "includeFunctionCall";
    public static final String INCLUDE_MATHEMATICAL_OPERATION = "includeMathematicalOperation";

    private DecisionExpressionAnalyzer() {}

    public static DecisionExpressionAnalysis analyze(Expression expression)
    {
        Visitor visitor = new Visitor();
        visitor.process(expression, null);
        return new DecisionExpressionAnalysis(visitor.includeFunctionCall, visitor.includeMathematicalOperation);
    }

    static class Visitor
            extends DefaultTraversalVisitor<Void>
    {
        private boolean includeFunctionCall;
        private boolean includeMathematicalOperation;

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void context)
        {
            includeFunctionCall = true;
            return super.visitFunctionCall(node, context);
        }

        @Override
        protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            includeMathematicalOperation = true;
            return super.visitArithmeticBinary(node, context);
        }

        @Override
        protected Void visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            includeMathematicalOperation = true;
            return super.visitArithmeticUnary(node, context);
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void context)
        {
            includeMathematicalOperation = true;
            return super.visitComparisonExpression(node, context);
        }
    }

    public record DecisionExpressionAnalysis(boolean includeFunctionCall, boolean includeMathematicalOperation)
    {
        public Map<String, String> toMap()
        {
            return Map.of(INCLUDE_FUNCTION_CALL, String.valueOf(includeFunctionCall), INCLUDE_MATHEMATICAL_OPERATION, String.valueOf(includeMathematicalOperation));
        }
    }
}
