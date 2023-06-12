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

package io.graphmdl.sqlrewrite;

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.SessionContext;
import io.graphmdl.sqlrewrite.analyzer.Analysis;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.SubscriptExpression;

import static java.util.Objects.requireNonNull;

public class AnyFunctionSqlRewrite
        implements GraphMDLRule
{
    public static final AnyFunctionSqlRewrite ANY_FUNCTION_SQL_REWRITE = new AnyFunctionSqlRewrite();

    @Override
    public Statement apply(Statement statement, SessionContext sessionContext, Analysis analysis, GraphMDL graphMDL)
    {
        return (Statement) new Rewriter().process(statement);
    }

    @Override
    public Statement apply(Statement statement, SessionContext sessionContext, GraphMDL graphMDL)
    {
        return apply(statement, sessionContext, null, graphMDL);
    }

    private static class Rewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitFunctionCall(FunctionCall node, Void context)
        {
            if (node.getName().toString().equalsIgnoreCase("any")) {
                return new SubscriptExpression(requireNonNull(node.getArguments().get(0)), new LongLiteral("1"));
            }
            return super.visitFunctionCall(node, context);
        }
    }
}
