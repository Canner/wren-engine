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

package io.accio.main.sql.duckdb;

import io.accio.base.sqlrewrite.BaseRewriter;
import io.accio.main.metadata.Metadata;
import io.accio.main.sql.SqlRewrite;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;

import static java.util.Objects.requireNonNull;

public class RewriteFunction
        implements SqlRewrite
{
    public static final RewriteFunction INSTANCE = new RewriteFunction();

    private RewriteFunction() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new RewriteFunctionRewriter(metadata).process(node, null);
    }

    private static class RewriteFunctionRewriter
            extends BaseRewriter<Void>
    {
        private final Metadata metadata;

        public RewriteFunctionRewriter(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        protected Node visitFunctionCall(FunctionCall node, Void context)
        {
            QualifiedName functionName = metadata.resolveFunction(node.getName().toString(), node.getArguments().size());

            FunctionCall newFunctionNode = FunctionCall.builder(node)
                    .name(functionName)
                    .build();

            return super.visitFunctionCall(newFunctionNode, context);
        }
    }
}
