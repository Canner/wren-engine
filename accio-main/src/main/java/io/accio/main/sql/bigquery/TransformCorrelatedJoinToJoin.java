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

import com.google.common.collect.ImmutableMap;
import io.accio.main.metadata.Metadata;
import io.accio.main.sql.SqlRewrite;
import io.accio.sqlrewrite.BaseRewriter;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SubqueryExpression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.QueryUtil.leftJoin;
import static java.util.Objects.requireNonNull;

/**
 * WARNING: It is not a complete implementation.
 * We're not willing to support correlated join, while some pg jdbc driver use correlated join in query.
 * TransformCorrelatedJoinToJoin rewrite correlated join to left join in specific cases.
 */
public class TransformCorrelatedJoinToJoin
        implements SqlRewrite
{
    public static final TransformCorrelatedJoinToJoin INSTANCE = new TransformCorrelatedJoinToJoin();

    private TransformCorrelatedJoinToJoin() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new TransformCorrelatedJoinToJoinRewriter().process(node, null);
    }

    private static class TransformCorrelatedJoinToJoinRewriter
            extends BaseRewriter<Map<NodeRef<SubqueryExpression>, CorrelationInfo>>
    {
        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Map<NodeRef<SubqueryExpression>, CorrelationInfo> context)
        {
            if (node.getFrom().isPresent() && node.getWhere().isPresent()) {
                Map<NodeRef<SubqueryExpression>, CorrelationInfo> subqueryExpressionReplaceInfos = FindSubQueryExpression.find(node.getWhere().get());
                if (subqueryExpressionReplaceInfos.size() > 0) {
                    Relation from = node.getFrom().get();
                    for (CorrelationInfo correlationInfo : subqueryExpressionReplaceInfos.values()) {
                        from = leftJoin(from, correlationInfo.getRelation(), correlationInfo.getJoinCriteria());
                    }
                    return new QuerySpecification(
                            node.getSelect(),
                            Optional.of(from),
                            node.getWhere().map(where -> (Expression) process(where, subqueryExpressionReplaceInfos)),
                            node.getGroupBy(),
                            node.getHaving(),
                            node.getWindows(),
                            node.getOrderBy(),
                            node.getOffset(),
                            node.getLimit());
                }
            }
            return node;
        }

        @Override
        protected Node visitSubqueryExpression(SubqueryExpression node, Map<NodeRef<SubqueryExpression>, CorrelationInfo> context)
        {
            if (context.containsKey(NodeRef.of(node))) {
                return context.get(NodeRef.of(node)).getSingleColumn().getExpression();
            }
            return node;
        }
    }

    private static class FindSubQueryExpression
            extends DefaultTraversalVisitor<Void>
    {
        private final Map<NodeRef<SubqueryExpression>, CorrelationInfo> replaceSubqueryWithExpression = new HashMap<>();

        private static Map<NodeRef<SubqueryExpression>, CorrelationInfo> find(Expression node)
        {
            FindSubQueryExpression findSubQueryExpression = new FindSubQueryExpression();
            findSubQueryExpression.process(node);
            return findSubQueryExpression.getReplaceSubqueryWithExpression();
        }

        private Map<NodeRef<SubqueryExpression>, CorrelationInfo> getReplaceSubqueryWithExpression()
        {
            return ImmutableMap.copyOf(replaceSubqueryWithExpression);
        }

        @Override
        protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            // make sure that subquery is SELECT ... FROM ...
            if (node.getQuery().getQueryBody() instanceof QuerySpecification) {
                QuerySpecification querySpecification = (QuerySpecification) node.getQuery().getQueryBody();
                if (querySpecification.getSelect().getSelectItems().size() == 1
                        && querySpecification.getSelect().getSelectItems().get(0) instanceof SingleColumn
                        && querySpecification.getFrom().isPresent()
                        && querySpecification.getWhere().isPresent()) {
                    replaceSubqueryWithExpression.put(
                            NodeRef.of(node),
                            new CorrelationInfo(
                                    (SingleColumn) querySpecification.getSelect().getSelectItems().get(0),
                                    querySpecification.getFrom().get(),
                                    new JoinOn(querySpecification.getWhere().get())));
                }
            }
            return null;
        }
    }

    private static class CorrelationInfo
    {
        private final SingleColumn singleColumn;
        private final Relation relation;
        private final JoinCriteria joinCriteria;

        private CorrelationInfo(SingleColumn singleColumn, Relation relation, JoinCriteria joinCriteria)
        {
            this.singleColumn = requireNonNull(singleColumn, "singleColumn is null");
            this.relation = requireNonNull(relation, "relation is null");
            this.joinCriteria = requireNonNull(joinCriteria, "joinCriteria is null");
        }

        private SingleColumn getSingleColumn()
        {
            return singleColumn;
        }

        private Relation getRelation()
        {
            return relation;
        }

        private JoinCriteria getJoinCriteria()
        {
            return joinCriteria;
        }
    }
}
