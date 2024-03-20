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

package io.wren.main.sql.bigquery;

import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.TimestampLiteral;
import io.wren.base.sqlrewrite.BaseRewriter;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;

import java.util.List;
import java.util.Optional;

/**
 * RewriteArithmetic is a class that rewrites arithmetic expressions to be compatible with BigQuery.
 * Because in BigQuery, TIMESTAMP +/- INTERVAL is not supported for intervals with non-zero MONTH or YEAR part, so we need to cast the TIMESTAMP to DATETIME as utc timezone.
 * <p>
 * select timestamp '2023-07-04 09:41:43.805201' + interval '1 year';
 * ->
 * select cast(timestamp '2023-07-04 09:41:43.805201' as datetime) + interval '1 year';
 */
public class RewriteArithmetic
        implements SqlRewrite
{
    public static final RewriteArithmetic INSTANCE = new RewriteArithmetic();
    public static final GenericDataType DATETIME = new GenericDataType(Optional.empty(), new Identifier("DATETIME"), List.of());

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new RewriteArithmeticRewriter().process(node, null);
    }

    public static class RewriteArithmeticRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            if (node.getLeft() instanceof IntervalLiteral && (node.getRight() instanceof TimestampLiteral)) {
                if (node.getLocation().isPresent()) {
                    return new ArithmeticBinaryExpression(
                            node.getLocation().get(),
                            node.getOperator(),
                            node.getLeft(),
                            new Cast(node.getRight(), DATETIME));
                }
                return new ArithmeticBinaryExpression(
                        node.getLocation().get(),
                        node.getOperator(),
                        node.getLeft(),
                        new Cast(node.getRight(), DATETIME));
            }
            else if (node.getLeft() instanceof TimestampLiteral && node.getRight() instanceof IntervalLiteral) {
                if (node.getLocation().isPresent()) {
                    return new ArithmeticBinaryExpression(
                            node.getLocation().get(),
                            node.getOperator(),
                            new Cast(node.getLeft(), DATETIME),
                            node.getRight());
                }
                return new ArithmeticBinaryExpression(
                        node.getLocation().get(),
                        node.getOperator(),
                        new Cast(node.getRight(), DATETIME),
                        node.getRight());
            }
            return node;
        }
    }
}
