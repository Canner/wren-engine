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

package io.accio.main.sql.bigquery;

import com.google.common.collect.ImmutableList;
import io.accio.base.sqlrewrite.BaseRewriter;
import io.accio.main.metadata.Metadata;
import io.accio.main.sql.SqlRewrite;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Remove column alias in alias relation.
 * <p>
 * e.g. SELECT * FROM (SELECT a FROM t) AS c(b)
 * -> SELECT * FROM (SELECT a AS b FROM t) AS c
 * <p>
 * e.g. WITH c(b) AS (SELECT a FROM t) SELECT b FROM c
 * -> WITH c AS (SELECT a AS b FROM t) SELECT b FROM c
 * <p>
 * Also convert values in alias relation to union all
 * e.g. SELECT * FROM (VALUES (1, 2), (3, 4)) t(a, b)
 * -> SELECT * FROM (SELECT 1 AS a, 2 AS b UNION ALL SELECT 3 AS a, 4 AS b) t
 * <p>
 * TODO: Currently we don't support
 *  - SELECT b FROM t1 AS t(b)
 */
public class RemoveColumnAliasInAliasRelation
        implements SqlRewrite
{
    public static final RemoveColumnAliasInAliasRelation INSTANCE = new RemoveColumnAliasInAliasRelation();

    private RemoveColumnAliasInAliasRelation() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new RemoveColumnAliasInAliasRelationRewriter().process(node, null);
    }

    private static class RemoveColumnAliasInAliasRelationRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitAliasedRelation(AliasedRelation node, Void context)
        {
            Relation relation = node.getRelation();
            List<Identifier> columnNames = node.getColumnNames() == null ? List.of() : node.getColumnNames();
            if (relation instanceof TableSubquery) {
                return new AliasedRelation(
                        new TableSubquery(replaceSingleColumnAliasInQuery(((TableSubquery) relation).getQuery(), columnNames)),
                        node.getAlias(),
                        List.of());
            }
            return node;
        }

        @Override
        protected Node visitWithQuery(WithQuery withQuery, Void context)
        {
            return new WithQuery(
                    withQuery.getName(),
                    replaceSingleColumnAliasInQuery(withQuery.getQuery(), withQuery.getColumnNames().orElse(List.of())),
                    Optional.empty());
        }
    }

    private static Query replaceSingleColumnAliasInQuery(Query query, List<Identifier> columnAliases)
    {
        QueryBody queryBody = query.getQueryBody();
        if (queryBody instanceof QuerySpecification && columnAliases.size() > 0) {
            QuerySpecification querySpecification = (QuerySpecification) queryBody;
            checkArgument(columnAliases.size() == querySpecification.getSelect().getSelectItems().size(),
                    "columnAliases size must be equal to selectItems size");
            List<SelectItem> selectItems = querySpecification.getSelect().getSelectItems();
            ImmutableList.Builder<SelectItem> newSelectItemsBuilder = ImmutableList.builder();
            for (int i = 0; i < selectItems.size(); i++) {
                if (selectItems.get(i) instanceof SingleColumn) {
                    // add value in columnAliases as alias for selectItem, if selectItem alias already exist, replace it
                    SingleColumn singleColumn = (SingleColumn) selectItems.get(i);
                    Identifier alias = columnAliases.get(i);
                    newSelectItemsBuilder.add(new SingleColumn(singleColumn.getExpression(), alias));
                }
            }
            query = new Query(
                    query.getWith(),
                    new QuerySpecification(
                            new Select(querySpecification.getSelect().isDistinct(), newSelectItemsBuilder.build()),
                            querySpecification.getFrom(),
                            querySpecification.getWhere(),
                            querySpecification.getGroupBy(),
                            querySpecification.getHaving(),
                            querySpecification.getWindows(),
                            querySpecification.getOrderBy(),
                            querySpecification.getOffset(),
                            querySpecification.getLimit()),
                    query.getOrderBy(),
                    query.getOffset(),
                    query.getLimit());
        }
        else if (queryBody instanceof Values) {
            Values values = (Values) queryBody;
            Union union = null;
            ImmutableList.Builder<Relation> newRowsBuilder = ImmutableList.builder();
            for (Expression rowExpr : values.getRows()) {
                if (rowExpr instanceof Row) {
                    Row row = (Row) rowExpr;
                    ImmutableList.Builder<SelectItem> singleColumnsBuilder = ImmutableList.builder();
                    for (int i = 0; i < row.getItems().size(); i++) {
                        singleColumnsBuilder.add(new SingleColumn(row.getItems().get(i), columnAliases.get(i)));
                    }
                    Select select = new Select(false, singleColumnsBuilder.build());
                    newRowsBuilder.add(new QuerySpecification(select, Optional.empty(), Optional.empty(),
                            Optional.empty(), Optional.empty(), List.of(), Optional.empty(), Optional.empty(), Optional.empty()));
                }
                else {
                    newRowsBuilder.add(new QuerySpecification(new Select(false, List.of(new SingleColumn(rowExpr, columnAliases.get(0)))),
                            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), List.of(),
                            Optional.empty(), Optional.empty(), Optional.empty()));
                }
                List<Relation> selects = newRowsBuilder.build();
                union = new Union(selects, false);
            }
            return new Query(
                    query.getWith(),
                    union,
                    query.getOrderBy(),
                    query.getOffset(),
                    query.getLimit());
        }
        return query;
    }
}
