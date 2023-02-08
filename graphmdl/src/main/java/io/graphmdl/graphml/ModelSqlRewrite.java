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
import io.graphmdl.graphml.base.dto.Model;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class ModelSqlRewrite
        implements GraphMLRule
{
    public static final ModelSqlRewrite MODEL_SQL_REWRITE = new ModelSqlRewrite();

    private ModelSqlRewrite() {}

    @Override
    public Node apply(Node root, Analysis analysis, GraphML graphML)
    {
        Map<String, Query> modelQueries =
                analysis.getModels().stream()
                        .collect(toUnmodifiableMap(Model::getName, Utils::parseModelSql));

        if (modelQueries.isEmpty()) {
            return root;
        }
        return new Rewriter(modelQueries, analysis).process(root);
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
    private static class Rewriter
            extends BaseVisitor
    {
        private final Map<String, Query> modelQueries;
        private final Analysis analysis;

        public Rewriter(Map<String, Query> modelQueries, Analysis analysis)
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

            return new Query(
                    node.getWith()
                            .map(with -> new With(
                                    with.isRecursive(),
                                    // model queries must come first since tables in with query should all be in order
                                    // e.g. WITH foo AS (SELECT * FROM a), bar AS (SELECT col FROM foo) SELECT * FROM bar
                                    // "bar" must come after "foo"
                                    Stream.concat(modelWithQueries.stream(), with.getQueries().stream())
                                            .collect(toUnmodifiableList())))
                            .or(() -> Optional.of(new With(false, modelWithQueries))),
                    node.getQueryBody(),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }
    }
}
