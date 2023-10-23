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

package io.accio.sqlrewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.accio.base.AccioMDL;
import io.accio.base.SessionContext;
import io.accio.base.dto.Metric;
import io.accio.base.dto.View;
import io.accio.sqlrewrite.analyzer.Analysis;
import io.accio.sqlrewrite.analyzer.StatementAnalyzer;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.accio.sqlrewrite.Utils.parseView;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class MetricViewSqlRewrite
        implements AccioRule
{
    public static final MetricViewSqlRewrite METRIC_VIEW_SQL_REWRITE = new MetricViewSqlRewrite();

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, AccioMDL accioMDL)
    {
        return apply(root, sessionContext, StatementAnalyzer.analyze(root, sessionContext, accioMDL), accioMDL);
    }

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, AccioMDL accioMDL)
    {
        MetricViewAnalyzer metricViewAnalyzer = new MetricViewAnalyzer(sessionContext, accioMDL);

        // analyze if the metric used by a view.
        analysis.getViews().stream().filter(view -> accioMDL.getView(view.getName()).isPresent())
                .forEach(metricViewAnalyzer::analyze);

        List<Analysis> allAnalysis = ImmutableList.<Analysis>builder()
                // the metricViewAnalyzer must be the first because the view maybe used by the view in analysis.
                .addAll(metricViewAnalyzer.getAnalyses())
                .add(analysis).build();

        Map<String, Query> metricQueries =
                allAnalysis.stream().flatMap(a -> a.getMetrics().stream())
                        .collect(toUnmodifiableMap(Metric::getName, Utils::parseMetricSql));

        Map<String, Query> metricRollupQueries =
                allAnalysis.stream().flatMap(a -> a.getMetricRollups().values().stream())
                        .collect(toUnmodifiableMap(rollup -> rollup.getMetric().getName(), Utils::parseMetricRollupSql));

        // The generation of views has a sequential order, with later views being able to reference earlier views.
        Map<String, Query> viewQueries = new LinkedHashMap<>();
        allAnalysis.stream().flatMap(a -> a.getViews().stream())
                .forEach(view -> viewQueries.put(view.getName(), parseView(view.getStatement())));

        return (Statement) new WithRewriter(metricQueries, metricRollupQueries, ImmutableMap.copyOf(viewQueries)).process(root);
    }

    private static class WithRewriter
            extends BaseRewriter<Void>
    {
        private final Map<String, Query> metricQueries;
        private final Map<String, Query> metricRollupQueries;

        private final Map<String, Query> viewQueries;

        public WithRewriter(
                Map<String, Query> metricQueries,
                Map<String, Query> metricRollupQueries,
                Map<String, Query> viewQueries)
        {
            this.metricQueries = requireNonNull(metricQueries, "metricQueries is null");
            this.metricRollupQueries = requireNonNull(metricRollupQueries, "metricRollupQueries is null");
            this.viewQueries = requireNonNull(viewQueries, "viewQueries is null");
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

            List<WithQuery> viewWithQueries = viewQueries.entrySet().stream()
                    .map(e -> new WithQuery(new Identifier(e.getKey()), e.getValue(), Optional.empty()))
                    .collect(toUnmodifiableList());

            List<WithQuery> withQueries = ImmutableList.<WithQuery>builder()
                    .addAll(metricWithQueries)
                    .addAll(metricRollupWithQueries)
                    .addAll(viewWithQueries)
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

    private static class MetricViewAnalyzer
    {
        ImmutableList.Builder<Analysis> analyses = ImmutableList.builder();
        private final SessionContext sessionContext;
        private final AccioMDL accioMDL;

        private MetricViewAnalyzer(SessionContext sessionContext, AccioMDL accioMDL)
        {
            this.sessionContext = sessionContext;
            this.accioMDL = accioMDL;
        }

        public void analyze(View view)
        {
            Analysis analysis = StatementAnalyzer.analyze(parseView(view.getStatement()), sessionContext, accioMDL);
            analyses.add(analysis);
            analysis.getViews().forEach(this::analyze);
        }

        public List<Analysis> getAnalyses()
        {
            return analyses.build();
        }
    }
}
