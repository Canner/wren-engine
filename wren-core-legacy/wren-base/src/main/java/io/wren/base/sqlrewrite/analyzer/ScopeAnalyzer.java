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

package io.wren.base.sqlrewrite.analyzer;

import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.Utils;

public class ScopeAnalyzer
{
    private ScopeAnalyzer() {}

    public static ScopeAnalysis analyze(WrenMDL wrenMDL, Node node, SessionContext sessionContext)
    {
        ScopeAnalysis analysis = new ScopeAnalysis();
        Visitor visitor = new Visitor(wrenMDL, analysis, sessionContext);
        visitor.process(node, null);
        return analysis;
    }

    static class Visitor
            extends DefaultTraversalVisitor<Void>
    {
        private final WrenMDL wrenMDL;
        private final ScopeAnalysis analysis;
        private final SessionContext sessionContext;

        public Visitor(WrenMDL wrenMDL, ScopeAnalysis analysis, SessionContext sessionContext)
        {
            this.wrenMDL = wrenMDL;
            this.analysis = analysis;
            this.sessionContext = sessionContext;
        }

        @Override
        protected Void visitTable(Table node, Void context)
        {
            if (isBelongToWren(node.getName())) {
                analysis.addUsedWrenObject(node);
            }
            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Void context)
        {
            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Void context)
        {
            analysis.addAliasedNode(node.getRelation(), node.getAlias().getValue());
            return super.visitAliasedRelation(node, context);
        }

        private boolean isBelongToWren(QualifiedName wrenObjectName)
        {
            CatalogSchemaTableName catalogSchemaTableName = Utils.toCatalogSchemaTableName(sessionContext, wrenObjectName);
            String tableName = catalogSchemaTableName.getSchemaTableName().getTableName();
            return catalogSchemaTableName.getCatalogName().equals(wrenMDL.getCatalog())
                    && catalogSchemaTableName.getSchemaTableName().getSchemaName().equals(wrenMDL.getSchema())
                    && (wrenMDL.listModels().stream().anyMatch(model -> model.getName().equals(tableName))
                    || wrenMDL.listMetrics().stream().anyMatch(metric -> metric.getName().equals(tableName)));
        }
    }
}
