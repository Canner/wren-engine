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

import io.cml.graphml.base.GraphML;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.With;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.sql.QueryUtil.nameReference;
import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.util.stream.Collectors.toUnmodifiableList;

public class RelationshipRewrite
        implements GraphMLRule
{
    public static final GraphMLRule RELATIONSHIP_REWRITE = new RelationshipRewrite();

    @Override
    public Node apply(Node root, Analyzer.Analysis analysis, GraphML graphML)
    {
        return new RelationshipRewrite.Rewriter(analysis).process(root);
    }

    private static class Rewriter
            extends BaseVisitor
    {
        private final Analyzer.Analysis analysis;

        Rewriter(Analyzer.Analysis analysis)
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
                    (QueryBody) process(node.getQueryBody()),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            QualifiedName qualifiedName = getQualifiedName(node);
            return getRegisteredRelationship(qualifiedName);
        }

        private Expression getRegisteredRelationship(QualifiedName node)
        {
            // TODO: catalog, schema, alias
            // check if first identifier is table
            Optional<QualifiedName> findTable = analysis.getTables().stream().filter(name -> name.equals(QualifiedName.of(node.getParts().get(0)))).findAny();
            if (findTable.isPresent()) {
                return getRegisteredRelationship(QualifiedName.of(node.getParts().subList(1, node.getParts().size())));
            }

            // find relationship cte
            Optional<String> candidate = analysis.getRelationshipCTEName(node.getPrefix().get().toString());
            if (candidate.isEmpty()) {
                return getRegisteredRelationship(QualifiedName.of(node.getParts().subList(0, node.getParts().size() - 1)));
            }
            return nameReference(candidate.get(), node.getSuffix());
        }
    }
}
