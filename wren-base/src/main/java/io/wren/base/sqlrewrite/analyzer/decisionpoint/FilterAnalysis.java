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

import static java.util.Objects.requireNonNull;

public abstract class FilterAnalysis
{
    public static FilterAnalysis and(FilterAnalysis left, FilterAnalysis right)
    {
        return new LogicalAnalysis(Type.AND, left, right);
    }

    public static FilterAnalysis or(FilterAnalysis left, FilterAnalysis right)
    {
        return new LogicalAnalysis(Type.OR, left, right);
    }

    public static FilterAnalysis expression(String node)
    {
        return new ExpressionAnalysis(node);
    }

    public enum Type
    {
        AND,
        OR,
        EXPR
    }

    private final Type type;

    public FilterAnalysis(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    public Type getType()
    {
        return type;
    }

    public static class LogicalAnalysis
            extends FilterAnalysis
    {
        private final FilterAnalysis left;
        private final FilterAnalysis right;

        public LogicalAnalysis(Type type, FilterAnalysis left, FilterAnalysis right)
        {
            super(type);
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        public FilterAnalysis getLeft()
        {
            return left;
        }

        public FilterAnalysis getRight()
        {
            return right;
        }
    }

    public static class ExpressionAnalysis
            extends FilterAnalysis
    {
        private final String node;

        public ExpressionAnalysis(String node)
        {
            super(Type.EXPR);
            this.node = requireNonNull(node, "node is null");
        }

        public String getNode()
        {
            return node;
        }
    }
}
