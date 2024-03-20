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

import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Statement;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.BaseTreeRewriter;
import io.wren.base.sqlrewrite.analyzer.Analysis;
import io.wren.base.sqlrewrite.analyzer.StatementAnalyzer;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;
import io.wren.main.sql.bigquery.analyzer.BigQueryTypeCoercion;

import static java.util.Objects.requireNonNull;

public class TypeCoercionRewrite
        implements SqlRewrite
{
    private final WrenMDL mdl;

    public TypeCoercionRewrite(WrenMDL mdl)
    {
        this.mdl = requireNonNull(mdl, "mdl is null");
    }

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        Analysis analysis = new Analysis((Statement) node);
        StatementAnalyzer.analyze(analysis, (Statement) node, SessionContext.builder()
                .setCatalog(mdl.getCatalog())
                .setSchema(mdl.getSchema())
                .build(), mdl, new BigQueryTypeCoercion(mdl));
        return new Rewriter(analysis).process(node);
    }

    static class Rewriter
            extends BaseTreeRewriter<Void>
    {
        private final Analysis analysis;

        public Rewriter(Analysis analysis)
        {
            this.analysis = analysis;
        }

        @Override
        protected Node visitExpression(Expression node, Void context)
        {
            NodeRef<Node> nodeRef = NodeRef.of(node);
            if (analysis.getTypeCoercionMap().containsKey(nodeRef)) {
                return analysis.getTypeCoercionMap().get(nodeRef);
            }
            return node;
        }
    }
}
