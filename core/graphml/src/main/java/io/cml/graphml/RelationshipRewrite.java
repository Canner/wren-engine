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

package io.cml.graphml;

import io.cml.graphml.analyzer.Analysis;
import io.cml.graphml.base.GraphML;
import io.trino.sql.QueryUtil;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.sql.QueryUtil.implicitJoin;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;

public class RelationshipRewrite
        implements GraphMLRule
{
    public static final GraphMLRule RELATIONSHIP_REWRITE = new RelationshipRewrite();

    @Override
    public Node apply(Node root, Analysis analysis, GraphML graphML)
    {
        return new RelationshipRewrite.Rewriter(analysis).process(root);
    }

    private static class Rewriter
            extends BaseVisitor
    {
        private final Analysis analysis;

        Rewriter(Analysis analysis)
        {
            this.analysis = analysis;
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            return new Query(
                    node.getWith()
                            .map(with -> new With(
                                    with.isRecursive(),
                                    Stream.concat(with.getQueries().stream(), analysis.getRelationshipCTE().values().stream())
                                            .collect(toUnmodifiableList())))
                            .or(() -> Optional.of(new With(false, analysis.getRelationshipCTE().values().stream().collect(toUnmodifiableList())))),
                    visitAndCast(node.getQueryBody()),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Void context)
        {
            Optional<Relation> from;
            List<Table> cteTables = analysis.getRelationshipCTE().values().stream()
                    .map(WithQuery::getName)
                    .map(Identifier::getValue)
                    .map(QualifiedName::of)
                    .map(QueryUtil::table)
                    .collect(toList());

            if (node.getFrom().isPresent()) {
                from = implicitJoinCTE(node.getFrom().get(), cteTables);
            }
            else {
                from = implicitJoinCTE(null, cteTables);
            }

            if (node.getLocation().isPresent()) {
                return new QuerySpecification(
                        node.getLocation().get(),
                        visitAndCast(node.getSelect()),
                        from,
                        node.getWhere().map(this::visitAndCast),
                        node.getGroupBy().map(this::visitAndCast),
                        node.getHaving().map(this::visitAndCast),
                        visitNodes(node.getWindows()),
                        node.getOrderBy().map(this::visitAndCast),
                        node.getOffset(),
                        node.getLimit());
            }

            return new QuerySpecification(
                    visitAndCast(node.getSelect()),
                    from,
                    node.getWhere().map(this::visitAndCast),
                    node.getGroupBy().map(this::visitAndCast),
                    node.getHaving().map(this::visitAndCast),
                    visitNodes(node.getWindows()),
                    node.getOrderBy().map(this::visitAndCast),
                    node.getOffset(),
                    node.getLimit());
        }

        private Optional<Relation> implicitJoinCTE(Relation left, List<Table> rights)
        {
            if (left != null) {
                for (Table right : rights) {
                    left = implicitJoin(left, right);
                }
                return Optional.of(left);
            }

            if (rights.size() > 1) {
                left = implicitJoin(rights.get(0), rights.get(1));
                for (Table right : rights.subList(2, rights.size())) {
                    left = implicitJoin(left, right);
                }
                return Optional.of(left);
            }

            if (rights.size() == 1) {
                return Optional.of(rights.get(0));
            }

            return Optional.empty();
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            return analysis.transferRelationship(NodeRef.of(node));
        }
    }
}
