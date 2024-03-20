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

import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Table;
import io.wren.base.sqlrewrite.BaseRewriter;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Remove catalog.schema prefix from column name.
 * e.g. rewrite SELECT catalog.schema.table.column FROM catalog.schema.table
 * to SELECT table.column FROM catalog.schema.table
 */
public class RemoveCatalogSchemaColumnPrefix
        implements SqlRewrite
{
    public static final RemoveCatalogSchemaColumnPrefix INSTANCE = new RemoveCatalogSchemaColumnPrefix();

    private RemoveCatalogSchemaColumnPrefix() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        FindColumnPrefixCandidatesVisitor visitor = new FindColumnPrefixCandidatesVisitor();
        visitor.process(node);
        return new RemoveRedundantColumnPrefixRewriter(visitor.getColumnPrefixCandidates()).process(node);
    }

    private static class RemoveRedundantColumnPrefixRewriter
            extends BaseRewriter<Void>
    {
        private final List<QualifiedName> columnPrefixCandidates;

        private RemoveRedundantColumnPrefixRewriter(List<QualifiedName> columnPrefixCandidates)
        {
            this.columnPrefixCandidates = requireNonNull(columnPrefixCandidates, "columnPrefixCandidates is null");
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            return columnPrefixCandidates.stream()
                    .filter(prefix ->
                            Optional.ofNullable(DereferenceExpression.getQualifiedName(node))
                                    .map(qualifiedName -> qualifiedName.toString().startsWith(prefix.toString()))
                                    .orElse(false))
                    .findFirst()
                    // extract [catalog].schema from table name
                    .map(tableName -> QualifiedName.of(tableName.getOriginalParts().subList(0, tableName.getOriginalParts().size() - 1)))
                    .map(qualifiedName -> trimDereferenceExpression(node, qualifiedName))
                    .orElse(node);
        }
    }

    private static DereferenceExpression trimDereferenceExpression(DereferenceExpression node, QualifiedName catalogSchemaPrefix)
    {
        QualifiedName qualifiedName = requireNonNull(DereferenceExpression.getQualifiedName(node), "qualifiedName is null");
        return (DereferenceExpression) DereferenceExpression.from(
                QualifiedName.of(
                        qualifiedName.getOriginalParts().subList(catalogSchemaPrefix.getOriginalParts().size(), qualifiedName.getOriginalParts().size())));
    }

    private static class FindColumnPrefixCandidatesVisitor
            extends DefaultTraversalVisitor<Void>
    {
        private final List<QualifiedName> columnPrefixCandidates = new ArrayList<>();

        private FindColumnPrefixCandidatesVisitor() {}

        private List<QualifiedName> getColumnPrefixCandidates()
        {
            return columnPrefixCandidates;
        }

        @Override
        protected Void visitTable(Table node, Void context)
        {
            QualifiedName tableName = node.getName();
            List<Identifier> tableNameParts = tableName.getOriginalParts();
            if (tableNameParts.size() == 2) {
                columnPrefixCandidates.add(tableName);
            }
            // this is full table name, catalog.schema.table or schema.table could be column prefix
            if (tableNameParts.size() == 3) {
                columnPrefixCandidates.add(QualifiedName.of(tableNameParts.subList(1, tableNameParts.size())));
                columnPrefixCandidates.add(tableName);
            }
            return null;
        }
    }
}
