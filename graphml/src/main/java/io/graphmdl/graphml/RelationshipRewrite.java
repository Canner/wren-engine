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

package io.graphmdl.graphml;

import io.graphmdl.graphml.analyzer.Analysis;
import io.graphmdl.graphml.base.GraphML;
import io.trino.sql.QueryUtil;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.sql.QueryUtil.table;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableList;

public class RelationshipRewrite
        implements GraphMLRule
{
    public static final GraphMLRule RELATIONSHIP_REWRITE = new RelationshipRewrite();

    @Override
    public Node apply(Node root, Analysis analysis, GraphML graphML)
    {
        Node rewriteWith = new WithRewriter(analysis).process(root);
        return new RelationshipRewrite.Rewriter(analysis).process(rewriteWith);
    }

    private static class WithRewriter
            extends BaseVisitor
    {
        private final Analysis analysis;

        WithRewriter(Analysis analysis)
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
                            .or(() ->
                                    analysis.getRelationshipCTE().values().isEmpty() ?
                                            Optional.empty() :
                                            Optional.of(new With(false, List.copyOf(analysis.getRelationshipCTE().values())))),
                    node.getQueryBody(),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }
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
        protected Node visitTable(Table node, Void context)
        {
            if (analysis.getReplaceTableWithCTEs().containsKey(NodeRef.of(node))) {
                Map<String, RelationshipCteGenerator.RelationshipCTEJoinInfo> relationshipInfoMapping = analysis.getRelationshipInfoMapping();
                Set<String> requiredRsCteName = analysis.getRelationshipFields().values().stream()
                        .map(expression -> expression.getBase().toString())
                        .collect(toSet());

                List<RelationshipCteGenerator.RelationshipCTEJoinInfo> cteTables = analysis.getReplaceTableWithCTEs().get(NodeRef.of(node)).stream()
                        .filter(name -> requiredRsCteName.contains(analysis.getRelationshipNameMapping().get(name)))
                        .map(name -> analysis.getRelationshipCTE().get(name))
                        .map(WithQuery::getName)
                        .map(Identifier::getValue)
                        .map(QualifiedName::of)
                        .map(name -> relationshipInfoMapping.get(name.toString()))
                        .collect(toUnmodifiableList());
                return leftJoin(node, cteTables);
            }
            return super.visitTable(node, context);
        }

        private Relation leftJoin(Relation left, List<RelationshipCteGenerator.RelationshipCTEJoinInfo> relationshipCTEJoinInfos)
        {
            for (RelationshipCteGenerator.RelationshipCTEJoinInfo info : relationshipCTEJoinInfos) {
                left = QueryUtil.leftJoin(left, table(QualifiedName.of(info.getCteName())), info.getCondition());
            }
            return left;
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            return analysis.getRelationshipFields().getOrDefault(NodeRef.of(node), node);
        }
    }
}
