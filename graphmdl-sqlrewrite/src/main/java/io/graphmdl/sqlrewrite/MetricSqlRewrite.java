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

import com.google.common.collect.ImmutableList;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.dto.Metric;
import io.graphmdl.sqlrewrite.analyzer.Analysis;
import io.graphmdl.sqlrewrite.analyzer.StatementAnalyzer;
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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class MetricSqlRewrite
        implements GraphMDLRule
{
    public static final MetricSqlRewrite METRIC_SQL_REWRITE = new MetricSqlRewrite();

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, GraphMDL graphMDL)
    {
        return apply(root, sessionContext, StatementAnalyzer.analyze(root, sessionContext, graphMDL), graphMDL);
    }

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, GraphMDL graphMDL)
    {
        Map<String, Query> metricQueries =
                analysis.getMetrics().stream()
                        .collect(toUnmodifiableMap(Metric::getName, Utils::parseMetricSql));

        Map<String, Query> metricRollupQueries =
                analysis.getMetricRollups().values().stream()
                        .collect(toUnmodifiableMap(rollup -> rollup.getMetric().getName(), Utils::parseMetricRollupSql));
        return (Statement) new WithRewriter(metricQueries, metricRollupQueries).process(root);
    }

    private static class WithRewriter
            extends BaseRewriter<Void>
    {
        private final Map<String, Query> metricQueries;
        private final Map<String, Query> metricRollupQueries;

        public WithRewriter(
                Map<String, Query> metricQueries,
                Map<String, Query> metricRollupQueries)
        {
            this.metricQueries = requireNonNull(metricQueries, "metricQueries is null");
            this.metricRollupQueries = requireNonNull(metricRollupQueries, "metricRollupQueries is null");
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            List<WithQuery> metricWithQueries = metricQueries.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()) // sort here to avoid test failed due to wrong with-query order
                    .map(e -> new WithQuery(new Identifier(e.getKey()), e.getValue(), Optional.empty()))
                    .collect(toUnmodifiableList());

            List<WithQuery> metricRollupWithQueries = metricRollupQueries.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()) // sort here to avoid test failed due to wrong with-query order
                    .map(e -> new WithQuery(new Identifier(e.getKey()), e.getValue(), Optional.empty()))
                    .collect(toUnmodifiableList());

            List<WithQuery> withQueries = ImmutableList.<WithQuery>builder()
                    .addAll(metricWithQueries)
                    .addAll(metricRollupWithQueries)
                    .build();

            return new Query(
                    node.getWith()
                            .map(with -> new With(
                                    with.isRecursive(),
                                    // model queries must come first since with-queries may use models
                                    // and tables in with query should all be in order.
                                    Stream.concat(withQueries.stream(), with.getQueries().stream())
                                            .collect(toUnmodifiableList())))
                            .or(() -> withQueries.isEmpty() ? Optional.empty() : Optional.of(new With(false, withQueries))),
                    node.getQueryBody(),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }
    }
}
