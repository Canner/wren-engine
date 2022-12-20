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
import io.trino.sql.QueryUtil;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
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

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.QueryUtil.implicitJoin;
import static io.trino.sql.QueryUtil.nameReference;
import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.util.stream.Collectors.toList;
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
            QualifiedName qualifiedName = getQualifiedName(node);
            return getRegisteredRelationship(qualifiedName);
        }

        private Expression getRegisteredRelationship(QualifiedName node)
        {
            // TODO: handling alias name
            
            if (node.getPrefix().isEmpty()) {
                return DereferenceExpression.from(node);
            }

            QualifiedName prefixRemoved = removeRelationPrefix(node);

            // find relationship cte
            return replaceRsPrefix(prefixRemoved);
        }

        private Expression replaceRsPrefix(QualifiedName node)
        {
            return replaceRsPrefix(node, node);
        }

        private Expression replaceRsPrefix(QualifiedName node, QualifiedName original)
        {
            if (node.getPrefix().isEmpty()) {
                return DereferenceExpression.from(original);
            }

            Optional<String> candidate = analysis.getRelationshipCTEName(node.getPrefix().get().toString());
            if (candidate.isEmpty()) {
                return replaceRsPrefix(QualifiedName.of(node.getParts().subList(0, node.getParts().size() - 1)), original);
            }
            return nameReference(candidate.get(), node.getSuffix());
        }

        private QualifiedName removeRelationPrefix(QualifiedName node)
        {
            return removeRelationPrefix(node, node);
        }

        private QualifiedName removeRelationPrefix(QualifiedName node, QualifiedName original)
        {
            if (node.getParts().size() == 0) {
                return original;
            }

            Optional<QualifiedName> matchedName = analysis.getTables().stream()
                    .filter(name -> findMatchTable(name, node))
                    .findAny();

            if (matchedName.isPresent()) {
                return QualifiedName.of(original.getParts().subList(node.getParts().size(), original.getParts().size()));
            }

            // doesn't match any candidate, end the matching
            if (node.getParts().size() == 1) {
                return original;
            }

            return removeRelationPrefix(QualifiedName.of(node.getParts().subList(0, node.getParts().size() - 1)), original);
        }

        private boolean findMatchTable(QualifiedName source, QualifiedName target)
        {
            checkArgument(source.getParts().size() <= 3, "Source name should not be over than 3 parts");
            checkArgument(target.getParts().size() > 0, "Target name doesn't have any part");
            if (source.getParts().size() == 3) {
                return isSameTable(source, target, 3);
            }
            else if (source.getParts().size() == 2) {
                return isSameTable(source, target, 2);
            }
            return isSameTable(source, target, 1);
        }

        private boolean isSameTable(QualifiedName source, QualifiedName target, int maxLength)
        {
            boolean result = false;
            for (int i = 0; i < maxLength; i++) {
                if (QualifiedName.of(source.getParts().subList(i, maxLength))
                        .equals(target)) {
                    result = true;
                    break;
                }
            }
            return result;
        }
    }
}
