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

package io.wren.main.sql.duckdb;

import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SubscriptExpression;
import io.wren.base.sqlrewrite.BaseRewriter;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;

public class RewriteArray
        implements SqlRewrite
{
    public static final RewriteArray INSTANCE = new RewriteArray();

    private RewriteArray() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new RewriteArrayRewriter().process(node, null);
    }

    private static class RewriteArrayRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            Expression base = node.getBase();
            if (base instanceof ArrayConstructor) {
                base = convertToArrayValue((ArrayConstructor) base);
            }
            if (node.getLocation().isPresent()) {
                return new SubscriptExpression(
                        node.getLocation().get(),
                        visitAndCast(base, context),
                        visitAndCast(node.getIndex(), context));
            }
            return new SubscriptExpression(visitAndCast(base, context), visitAndCast(node.getIndex(), context));
        }

        private Expression convertToArrayValue(ArrayConstructor node)
        {
            if (node.getLocation().isPresent()) {
                return new FunctionCall(
                        node.getLocation().get(),
                        QualifiedName.of("array_value"),
                        node.getValues());
            }
            return new FunctionCall(
                    QualifiedName.of("array_value"),
                    node.getValues());
        }
    }
}
