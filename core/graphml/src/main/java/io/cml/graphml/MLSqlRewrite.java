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

import io.cml.graphml.Analyzer.Analysis;
import io.cml.graphml.dto.Model;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.cml.graphml.ModelUtils.getModelSql;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class MLSqlRewrite
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private MLSqlRewrite() {}

    public static String rewrite(String sql, GraphML graphML)
    {
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        Analysis analysis = Analyzer.analyze(statement);
        Map<String, Query> modelQueries = graphML.listModels().stream()
                .filter(model -> analysis.getTables().stream().anyMatch(table -> model.getName().equals(table.toString())))
                .collect(toUnmodifiableMap(Model::getName, MLSqlRewrite::parseModelSql));

        if (modelQueries.isEmpty()) {
            return sql;
        }
        return SqlFormatter.formatSql(new MLRewriter(modelQueries).process(statement));
    }

    private static Query parseModelSql(Model model)
    {
        String sql = getModelSql(model);
        Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        if (statement instanceof Query) {
            return (Query) statement;
        }
        throw new IllegalArgumentException(format("model %s is not a query, sql %s", model.getName(), sql));
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
    private static class MLRewriter
            extends BaseVisitor
    {
        private final Map<String, Query> modelQueries;

        public MLRewriter(Map<String, Query> modelQueries)
        {
            this.modelQueries = requireNonNull(modelQueries, "modelQueries is null");
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
