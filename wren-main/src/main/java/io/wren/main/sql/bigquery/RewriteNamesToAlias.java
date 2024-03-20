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

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.wren.base.sqlrewrite.BaseRewriter;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * If a query contains aliases in the SELECT clause, those aliases override names in the corresponding FROM clause.
 * So need to rewrite BigQuery group by clause and order by clause to use aliases.
 * SELECT
 * FLOOR(l_orderkey) new_orderkey,
 * COUNT(*) count
 * FROM
 * tpch_tiny.lineitem
 * GROUP BY FLOOR(l_orderkey)
 * ORDER BY FLOOR(l_orderkey) ASC
 * ->
 * SELECT
 * FLOOR(l_orderkey) new_orderkey,
 * COUNT(*) count
 * FROM
 * tpch_tiny.lineitem
 * GROUP BY new_orderkey
 * ORDER BY new_orderkey ASC
 */
public class RewriteNamesToAlias
        implements SqlRewrite
{
    public static final RewriteNamesToAlias INSTANCE = new RewriteNamesToAlias();

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new RewriteNamesToAliasRewriter().process(node, null);
    }

    public static class RewriteNamesToAliasRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Void context)
        {
            Map<Expression, Identifier> selectItemAliasMap = new HashMap<>();
            for (SelectItem selectItem : node.getSelect().getSelectItems()) {
                if (selectItem instanceof SingleColumn) {
                    SingleColumn singleColumn = (SingleColumn) selectItem;
                    if (singleColumn.getAlias().isPresent()) {
                        selectItemAliasMap.putIfAbsent(singleColumn.getExpression(), singleColumn.getAlias().get());
                    }
                }
            }

            Optional<OrderBy> newOrderBy = node.getOrderBy().isEmpty() ? Optional.empty() : Optional.of(rewriteOrderBy(selectItemAliasMap, node.getOrderBy().get()));
            Optional<GroupBy> newGroupBy = node.getGroupBy().isEmpty() ? Optional.empty() : Optional.of(rewriteGroupBy(selectItemAliasMap, node.getGroupBy().get()));

            if (node.getLocation().isPresent()) {
                return new QuerySpecification(
                        node.getLocation().get(),
                        node.getSelect(),
                        node.getFrom(),
                        node.getWhere(),
                        newGroupBy,
                        node.getHaving(),
                        node.getWindows(),
                        newOrderBy,
                        node.getOffset(),
                        node.getLimit());
            }
            return new QuerySpecification(
                    node.getSelect(),
                    node.getFrom(),
                    node.getWhere(),
                    newGroupBy,
                    node.getHaving(),
                    node.getWindows(),
                    newOrderBy,
                    node.getOffset(),
                    node.getLimit());
        }

        private static OrderBy rewriteOrderBy(Map<Expression, Identifier> selectItemAliasMap, OrderBy orderBy)
        {
            ImmutableList.Builder<SortItem> sortItemBuilder = ImmutableList.builder();
            for (SortItem sortItem : orderBy.getSortItems()) {
                Expression sortKey = sortItem.getSortKey();
                if (selectItemAliasMap.containsKey(sortKey)) {
                    if (sortItem.getLocation().isPresent()) {
                        sortItemBuilder.add(new SortItem(sortItem.getLocation().get(), selectItemAliasMap.get(sortKey), sortItem.getOrdering(), sortItem.getNullOrdering()));
                    }
                    else {
                        sortItemBuilder.add(new SortItem(selectItemAliasMap.get(sortKey), sortItem.getOrdering(), sortItem.getNullOrdering()));
                    }
                }
                else {
                    sortItemBuilder.add(sortItem);
                }
            }

            if (orderBy.getLocation().isPresent()) {
                return new OrderBy(orderBy.getLocation().get(), sortItemBuilder.build());
            }
            return new OrderBy(sortItemBuilder.build());
        }

        private static GroupBy rewriteGroupBy(Map<Expression, Identifier> selectItemAliasMap, GroupBy node)
        {
            ImmutableList.Builder<GroupingElement> groupingElementBuilder = ImmutableList.builder();
            for (GroupingElement groupingElement : node.getGroupingElements()) {
                if (groupingElement instanceof SimpleGroupBy) {
                    groupingElementBuilder.add(rewriteSimpleGroupBy(selectItemAliasMap, (SimpleGroupBy) groupingElement));
                }
                else {
                    groupingElementBuilder.add(groupingElement);
                }
            }

            if (node.getLocation().isPresent()) {
                return new GroupBy(
                        node.getLocation().get(),
                        node.isDistinct(),
                        groupingElementBuilder.build());
            }
            return new GroupBy(
                    node.isDistinct(),
                    groupingElementBuilder.build());
        }

        private static GroupingElement rewriteSimpleGroupBy(Map<Expression, Identifier> selectItemAliasMap, SimpleGroupBy node)
        {
            ImmutableList.Builder<Expression> simpleGroupByBuilder = ImmutableList.builder();
            for (Expression expression : node.getExpressions()) {
                if (selectItemAliasMap.containsKey(expression)) {
                    simpleGroupByBuilder.add(selectItemAliasMap.get(expression));
                }
                else {
                    simpleGroupByBuilder.add(expression);
                }
            }
            return new SimpleGroupBy(simpleGroupByBuilder.build());
        }
    }
}
