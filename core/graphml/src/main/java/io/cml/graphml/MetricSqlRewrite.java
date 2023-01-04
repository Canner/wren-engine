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
import io.cml.graphml.base.dto.Metric;
import io.cml.graphml.base.dto.Model;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class MetricSqlRewrite
        implements GraphMLRule
{
    public static final MetricSqlRewrite METRIC_SQL_REWRITE = new MetricSqlRewrite();

    @Override
    public Node apply(Node root, Analysis analysis, GraphML graphML)
    {
        Map<String, Query> metricQueries = graphML.listMetrics().stream()
                .filter(metric -> analysis.getTables().stream().anyMatch(table -> metric.getName().equals(table.toString())))
                .collect(toUnmodifiableMap(Metric::getName, Utils::parseMetricSql));

        Map<String, Query> baseModelQueries = graphML.listMetrics().stream()
                // only process the visited metric
                .filter(metric -> analysis.getTables().stream().anyMatch(table -> metric.getName().equals(table.toString())))
                .map(Metric::getBaseModel).distinct()
                // filter the model processed by ModelSqlRewrite
                .filter(model -> !analysis.getTables().contains(model))
                .map(model -> graphML.getModel(model).orElseThrow(() -> new IllegalArgumentException(format("model %s is not found", model))))
                .collect(toUnmodifiableMap(Model::getName, Utils::parseModelSql));
        return new MetricSqlRewrite.Rewriter(metricQueries, baseModelQueries, analysis).process(root);
    }

    private static class Rewriter
            extends BaseVisitor
    {
        private final Map<String, Query> metricQueries;
        private final Map<String, Query> baseModelQueries;
        private final Analysis analysis;

        public Rewriter(
                Map<String, Query> metricQueries,
                Map<String, Query> baseModelQueries,
                Analysis analysis)
        {
            this.metricQueries = metricQueries;
            this.analysis = analysis;
            this.baseModelQueries = baseModelQueries;
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            List<WithQuery> metricWithQueries = metricQueries.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()) // sort here to avoid test failed due to wrong with-query order
                    .map(e -> new WithQuery(new Identifier(e.getKey()), e.getValue(), Optional.empty()))
                    .collect(toUnmodifiableList());

            List<WithQuery> baseModelWithQueries = baseModelQueries.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()) // sort here to avoid test failed due to wrong with-query order
                    .map(e -> new WithQuery(new Identifier(e.getKey()), e.getValue(), Optional.empty()))
                    .collect(toUnmodifiableList());

            // The base model should be first.
            Stream<WithQuery> orderedWithQuery = Stream.concat(baseModelWithQueries.stream(), metricWithQueries.stream());

            return new Query(
                    node.getWith()
                            .map(with -> new With(
                                    with.isRecursive(),
                                    Stream.concat(orderedWithQuery, with.getQueries().stream())
                                            .collect(toUnmodifiableList())))
                            .or(() -> Optional.of(new With(false, orderedWithQuery.collect(toUnmodifiableList())))),
                    node.getQueryBody(),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }
    }
}
