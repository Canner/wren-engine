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

package io.graphmdl.sqlrewrite;

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.dto.Model;
import io.graphmdl.sqlrewrite.analyzer.Analysis;
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.sql.QueryUtil.table;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class ModelSqlRewrite
        implements GraphMDLRule
{
    public static final ModelSqlRewrite MODEL_SQL_REWRITE = new ModelSqlRewrite();

    private ModelSqlRewrite() {}

    @Override
    public Node apply(Node root, SessionContext sessionContext, Analysis analysis, GraphMDL graphMDL)
    {
        Map<String, Query> modelQueries =
                analysis.getModels().stream()
                        .collect(toUnmodifiableMap(Model::getName, Utils::parseModelSql));

        if (modelQueries.isEmpty()) {
            return root;
        }

        Node rewriteWith = new WithRewriter(modelQueries, analysis).process(root);
        return new Rewriter(analysis).process(rewriteWith);
    }

    /**
     * In MLRewriter, we will add all participated model sql in WITH-QUERY, and rewrite
     * all tables that are models to TableSubQuery in WITH-QUERYs
     * <p>
     * e.g. Given model "foo" and its reference sql is SELECT * FROM t1
     * <pre>
     *     SELECT * FROM foo
     * </pre>
     * will be rewritten to
     * <pre>
     *     WITH foo AS (SELECT * FROM t1)
     *     SELECT * FROM foo
     * </pre>
     * and
     * <pre>
     *     WITH a AS (SELECT * FROM foo)
     *     SELECT * FROM a JOIN b on a.id=b.id
     * </pre>
     * will be rewritten to
     * <pre>
     *     WITH foo AS (SELECT * FROM t1),
     *          a AS (SELECT * FROM foo)
     *     SELECT * FROM a JOIN b on a.id=b.id
     * </pre>
     */
    private static class WithRewriter
            extends BaseVisitor
    {
        private final Map<String, Query> modelQueries;
        private final Analysis analysis;

        public WithRewriter(Map<String, Query> modelQueries, Analysis analysis)
        {
            this.modelQueries = requireNonNull(modelQueries, "modelQueries is null");
            this.analysis = requireNonNull(analysis, "analysis is null");
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            List<WithQuery> modelWithQueries = modelQueries.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()) // sort here to avoid test failed due to wrong with-query order
                    .map(e -> new WithQuery(new Identifier(e.getKey()), e.getValue(), Optional.empty()))
                    .collect(toUnmodifiableList());

            Collection<WithQuery> relationshipCTEs = analysis.getRelationshipCTE().values();

            List<WithQuery> withQueries =
                    Stream.concat(modelWithQueries.stream(), relationshipCTEs.stream())
                            .collect(toUnmodifiableList());

            return new Query(
                    node.getWith()
                            .map(with -> new With(
                                    with.isRecursive(),
                                    // model queries must come first since with-queries may use models
                                    // and tables in with query should all be in order.
                                    Stream.concat(withQueries.stream(), with.getQueries().stream())
                                            .collect(toUnmodifiableList())))
                            .or(() -> Optional.of(new With(false, withQueries))),
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
            Node result = node;
            if (analysis.getModelNodeRefs().contains(NodeRef.of(node))) {
                result = applyModelRule(node);
            }

            Set<String> relationshipCTENames = analysis.getReplaceTableWithCTEs().getOrDefault(NodeRef.of(node), Set.of());
            if (relationshipCTENames.size() > 0) {
                result = applyRelationshipRule((Table) result, relationshipCTENames);
            }

            return result;
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            return analysis.getRelationshipFields().getOrDefault(NodeRef.of(node), node);
        }

        // the model is added in with query, and the catalog and schema should be removed
        private Node applyModelRule(Table table)
        {
            return new Table(QualifiedName.of(table.getName().getSuffix()));
        }

        private Relation applyRelationshipRule(Table table, Set<String> relationshipCTENames)
        {
            Map<String, RelationshipCteGenerator.RelationshipCTEJoinInfo> relationshipInfoMapping = analysis.getRelationshipInfoMapping();
            Set<String> requiredRsCteName = analysis.getRelationshipFields().values().stream()
                    .map(expression -> expression.getBase().toString())
                    .collect(toSet());

            List<RelationshipCteGenerator.RelationshipCTEJoinInfo> cteTables =
                    relationshipCTENames.stream()
                            .filter(name -> requiredRsCteName.contains(analysis.getRelationshipNameMapping().get(name)))
                            .map(name -> analysis.getRelationshipCTE().get(name))
                            .map(WithQuery::getName)
                            .map(Identifier::getValue)
                            .map(QualifiedName::of)
                            .map(name -> relationshipInfoMapping.get(name.toString()))
                            .collect(toUnmodifiableList());

            return leftJoin(table, cteTables);
        }

        private static Relation leftJoin(Relation left, List<RelationshipCteGenerator.RelationshipCTEJoinInfo> relationshipCTEJoinInfos)
        {
            for (RelationshipCteGenerator.RelationshipCTEJoinInfo info : relationshipCTEJoinInfos) {
                left = QueryUtil.leftJoin(left, table(QualifiedName.of(info.getCteName())), info.getCondition());
            }
            return left;
        }
    }
}
