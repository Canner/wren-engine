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

package io.graphmdl.main.sql.bigquery;

import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.main.sql.SqlRewrite;
import io.graphmdl.sqlrewrite.BaseRewriter;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.WindowDefinition;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * BigQuery only supports one dimension array in unnest and doesn't support column alias in alias relation.
 * ReplaceColumnAliasInUnnest will replace table alias with column alias in unnest alias relation.
 * e.g. SELECT a.id FROM UNNEST(ARRAY[1]) as a(id)
 * -> SELECT id FROM UNNEST(ARRAY[1]) AS id
 * Note that example above is not valid in postgresql, but it is valid in BigQuery syntax.
 */
public class ReplaceColumnAliasInUnnest
        implements SqlRewrite
{
    public static final ReplaceColumnAliasInUnnest INSTANCE = new ReplaceColumnAliasInUnnest();

    private ReplaceColumnAliasInUnnest() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new ReplaceColumnAliasInUnnestRewriter().process(node, null);
    }

    private static class ReplaceColumnAliasInUnnestRewriter
            extends BaseRewriter<DereferenceExpression>
    {
        @Override
        protected Node visitQuerySpecification(QuerySpecification node, DereferenceExpression context)
        {
            if (node.getFrom().isPresent()
                    && node.getFrom().get() instanceof AliasedRelation
                    && ((AliasedRelation) node.getFrom().get()).getRelation() instanceof TableSubquery) {
                return super.visitQuerySpecification(node, context);
            }

            DereferenceExpression unnestColumnAlias = node.getFrom().map(FindUnnestAlias::find).orElse(null);
            return new QuerySpecification(
                    (Select) process(node.getSelect(), unnestColumnAlias),
                    node.getFrom().map(from -> (Relation) process(from, unnestColumnAlias)),
                    node.getWhere().map(where -> (Expression) process(where, unnestColumnAlias)),
                    node.getGroupBy().map(groupBy -> (GroupBy) process(groupBy, unnestColumnAlias)),
                    node.getHaving().map(having -> (Expression) process(having, unnestColumnAlias)),
                    node.getWindows().stream().map(window -> (WindowDefinition) process(window, unnestColumnAlias)).collect(toImmutableList()),
                    node.getOrderBy().map(orderBy -> (OrderBy) process(orderBy, unnestColumnAlias)),
                    node.getOffset().map(offset -> (Offset) process(offset, unnestColumnAlias)),
                    node.getLimit().map(limit -> process(limit, unnestColumnAlias)));
        }

        @Override
        protected Node visitAliasedRelation(AliasedRelation node, DereferenceExpression context)
        {
            // bigquery only support one column in unnest, replace the column alias with the table alias here
            // e.g. unnest(ARRAY[1]) as t(c) -> unnest(ARRAY[1]) as c
            if (node.getRelation() instanceof Unnest && node.getColumnNames() != null && node.getColumnNames().size() == 1) {
                return new AliasedRelation(
                        node.getRelation(),
                        node.getColumnNames().get(0),
                        List.of());
            }
            return super.visitAliasedRelation(node, context);
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, DereferenceExpression context)
        {
            return node.equals(context) ? context.getField() : node;
        }
    }

    private static class FindUnnestAlias
            extends DefaultTraversalVisitor<Void>
    {
        private DereferenceExpression unnestColumnAlias;

        private static DereferenceExpression find(Node node)
        {
            FindUnnestAlias findUnnestAlias = new FindUnnestAlias();
            findUnnestAlias.process(node, null);
            return findUnnestAlias.getUnnestColumnAlias();
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Void context)
        {
            if (node.getRelation() instanceof Unnest && node.getColumnNames() != null && node.getColumnNames().size() == 1) {
                unnestColumnAlias = new DereferenceExpression(node.getAlias(), node.getColumnNames().get(0));
            }
            return process(node.getRelation(), context);
        }

        private DereferenceExpression getUnnestColumnAlias()
        {
            return unnestColumnAlias;
        }
    }
}
