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

package io.wren.main.sql.snowflake;

import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.TypeParameter;
import io.wren.base.sqlrewrite.BaseRewriter;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;

import java.util.List;
import java.util.Optional;

public class RewriteCast
        implements SqlRewrite
{
    public static final RewriteCast INSTANCE = new RewriteCast();

    private RewriteCast() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new CastRewriter().process(node, null);
    }

    private static class CastRewriter
            extends BaseRewriter<Void>
    {
        private boolean targetDataTypeIsBytea;

        @Override
        protected Node visitCast(Cast node, Void context)
        {
            targetDataTypeIsBytea = isBytea(node.getType()) || isArrayBytea(node.getType());

            if (node.getLocation().isPresent()) {
                return new Cast(
                        node.getLocation().get(),
                        visitAndCast(node.getExpression(), context),
                        visitAndCast(node.getType(), context),
                        node.isSafe(),
                        node.isTypeOnly());
            }
            return new Cast(
                    visitAndCast(node.getExpression(), context),
                    visitAndCast(node.getType(), context),
                    node.isSafe(),
                    node.isTypeOnly());
        }

        @Override
        protected Node visitStringLiteral(StringLiteral node, Void context)
        {
            if (targetDataTypeIsBytea) {
                return hexEncode(node);
            }
            return super.visitStringLiteral(node, context);
        }

        private boolean isBytea(DataType type)
        {
            return type instanceof GenericDataType gdType &&
                    gdType.getName().getValue().equalsIgnoreCase("bytea");
        }

        private boolean isArrayBytea(DataType type)
        {
            return type instanceof GenericDataType gdType &&
                    gdType.getName().getValue().equalsIgnoreCase("array") &&
                    gdType.getArguments().size() == 1 &&
                    gdType.getArguments().getFirst() instanceof TypeParameter param &&
                    isBytea(param.getValue());
        }

        private Expression hexEncode(Expression node)
        {
            return new FunctionCall(
                    node.getLocation(),
                    QualifiedName.of("HEX_ENCODE"),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    List.of(node));
        }
    }
}
