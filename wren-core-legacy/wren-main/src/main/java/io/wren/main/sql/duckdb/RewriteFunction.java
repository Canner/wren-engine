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

import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.wren.base.sqlrewrite.BaseRewriter;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.wren.main.connector.duckdb.DuckDBMetadata.PG_TO_DUCKDB_FUNCTION_NAME_MAPPINGS;
import static java.util.Locale.ENGLISH;

public class RewriteFunction
        implements SqlRewrite
{
    public static final RewriteFunction INSTANCE = new RewriteFunction();

    private RewriteFunction() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new RewriteFunctionRewriter().process(node, null);
    }

    private static class RewriteFunctionRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitFunctionCall(FunctionCall node, Void context)
        {
            QualifiedName functionName = resolveFunction(node.getName().toString());

            FunctionCall newFunctionNode = FunctionCall.builder(node)
                    .name(functionName)
                    .build();

            return super.visitFunctionCall(newFunctionNode, context);
        }

        private QualifiedName resolveFunction(String functionName)
        {
            List<String> funcNameLowerCase = Arrays.stream(functionName.toLowerCase(ENGLISH).split("\\.")).collect(Collectors.toList());
            QualifiedName qualifiedName = QualifiedName.of(funcNameLowerCase);
            if (PG_TO_DUCKDB_FUNCTION_NAME_MAPPINGS.containsKey(qualifiedName.getSuffix())) {
                return QualifiedName.of(PG_TO_DUCKDB_FUNCTION_NAME_MAPPINGS.get(qualifiedName.getSuffix()));
            }
            return qualifiedName;
        }
    }
}
