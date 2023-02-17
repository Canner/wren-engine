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
import io.graphmdl.base.dto.Metric;
import io.graphmdl.sqlrewrite.analyzer.Analysis;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class MetricSqlRewrite
        implements GraphMDLRule
{
    public static final MetricSqlRewrite METRIC_SQL_REWRITE = new MetricSqlRewrite();

    @Override
    public Node apply(Node root, SessionContext context, Analysis analysis, GraphMDL graphMDL)
    {
        Map<String, Query> metricQueries = graphMDL.listMetrics().stream()
                .filter(metric ->
                        analysis.getTables().stream()
                                .filter(table -> table.getCatalogName().equals(graphMDL.getCatalog()))
                                .filter(table -> table.getSchemaTableName().getSchemaName().equals(graphMDL.getSchema()))
                                .anyMatch(table -> table.getSchemaTableName().getTableName().equals(metric.getName())))
                .collect(toUnmodifiableMap(Metric::getName, Utils::parseMetricSql));
        return new MetricSqlRewrite.Rewriter(metricQueries, analysis).process(root);
    }

    private static class Rewriter
            extends BaseVisitor
    {
        private final Map<String, Query> metricQueries;
        private final Analysis analysis;

        public Rewriter(
                Map<String, Query> metricQueries,
                Analysis analysis)
        {
            this.metricQueries = metricQueries;
            this.analysis = analysis;
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            List<WithQuery> metricWithQueries = metricQueries.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()) // sort here to avoid test failed due to wrong with-query order
                    .map(e -> new WithQuery(new Identifier(e.getKey()), e.getValue(), Optional.empty()))
                    .collect(toUnmodifiableList());

            return new Query(
                    node.getWith()
                            .map(with -> new With(
                                    with.isRecursive(),
                                    Stream.concat(with.getQueries().stream(), metricWithQueries.stream())
                                            .collect(toUnmodifiableList())))
                            .or(() -> Optional.ofNullable(metricWithQueries.isEmpty() ? null : new With(false, metricWithQueries))),
                    node.getQueryBody(),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }
    }
}
