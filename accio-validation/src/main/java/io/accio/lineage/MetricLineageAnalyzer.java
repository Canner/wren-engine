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

package io.accio.lineage;

import io.accio.Utils;
import io.accio.base.AccioMDL;
import io.accio.base.dto.AccioObject;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.airlift.log.Logger;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.SubscriptExpression;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.builder.GraphTypeBuilder;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MetricLineageAnalyzer
{
    private static final Logger LOG = Logger.get(MetricLineageAnalyzer.class);

    private MetricLineageAnalyzer() {}

    public static Graph<AccioObject, DefaultEdge> analyze(AccioMDL accioMDL)
    {
        Graph<AccioObject, DefaultEdge> graph = createSimpleDirectedGraph();
        accioMDL.listModels().forEach(graph::addVertex);
        accioMDL.listMetrics().forEach(graph::addVertex);

        // analyze every column in every metric
        for (Metric metric : accioMDL.listMetrics()) {
            LOG.debug("Analyzing metric %s", metric.getName());
            ExpressionVisitor expressionVisitor = new ExpressionVisitor(graph, accioMDL, metric);
            Model baseModel = accioMDL.getModel(metric.getBaseModel()).orElseThrow(() -> new RuntimeException(format("Model %s not found", metric.getBaseModel())));
            graph.addEdge(baseModel, metric);
            metric.getDimension().forEach(column -> expressionVisitor.process(Utils.parseExpression(column.getExpression().orElse(column.getName())), metric));
        }
        return graph;
    }

    private static Graph<AccioObject, DefaultEdge> createSimpleDirectedGraph()
    {
        return GraphTypeBuilder
                .<AccioObject, DefaultEdge>directed()
                .allowingMultipleEdges(false)
                .allowingSelfLoops(false)
                .weighted(false)
                .edgeClass(DefaultEdge.class)
                .buildGraph();
    }

    private static class ExpressionVisitor
            extends AstVisitor<AccioObject, AccioObject>
    {
        private final Graph<AccioObject, DefaultEdge> graph;
        private final AccioMDL accioMDL;
        private final AccioObject baseObject;

        private ExpressionVisitor(Graph<AccioObject, DefaultEdge> graph, AccioMDL accioMDL, AccioObject baseObject)
        {
            this.graph = requireNonNull(graph, "graph is null");
            this.accioMDL = requireNonNull(accioMDL, "accioMDL is null");
            this.baseObject = requireNonNull(baseObject, "baseObject is null");
        }

        @Override
        protected AccioObject visitDereferenceExpression(DereferenceExpression node, AccioObject baseObject)
        {
            Expression first = getFirst(node);
            AccioObject firstObject = process(first, baseObject);

            Expression remainder = Utils.removePrefix(node, 1);
            process(remainder, firstObject);
            return baseObject;
        }

        @Override
        protected AccioObject visitIdentifier(Identifier node, AccioObject context)
        {
            String resolved;
            if (context instanceof Metric) {
                Metric metric = (Metric) context;
                resolved = accioMDL.getModel(metric.getBaseModel())
                        .orElseThrow(() -> new RuntimeException(format("Model %s not found", metric.getBaseModel())))
                        .getColumnNames().stream().filter(columnName -> columnName.equals(node.getValue()))
                        .findAny().orElseThrow(() -> new RuntimeException(format("Column %s not found in %s", node.getValue(), metric.getBaseModel())));
            }
            else {
                resolved = context.getColumnNames().stream().filter(columnName -> columnName.equals(node.getValue()))
                        .findAny().orElseThrow(() -> new RuntimeException(format("Column %s not found in %s", node.getValue(), context.getName())));
            }

            Optional<Model> modelOptional = accioMDL.getModel(context.getName());
            if (modelOptional.isPresent()) {
                Optional<AccioObject> relatedOptional = modelOptional.get().getColumns().stream()
                        .filter(column -> column.getName().equals(resolved))
                        .filter(column -> column.getRelationship().isPresent())
                        .findAny()
                        .map(column -> accioMDL.getModel(column.getType()).orElseThrow(() -> new RuntimeException(format("Model %s not found", column.getType()))));
                relatedOptional
                        .ifPresent(relatedModel -> graph.addEdge(relatedModel, baseObject));
                return relatedOptional.orElse(context);
            }

            Optional<Model> baseModelOptional = accioMDL.getMetric(context.getName())
                    .map(metric -> accioMDL.getModel(metric.getBaseModel()).orElseThrow(() -> new RuntimeException(format("Model %s not found", metric.getBaseModel()))));
            if (baseModelOptional.isPresent()) {
                Optional<AccioObject> relatedOptional = baseModelOptional.get().getColumns().stream()
                        .filter(column -> column.getName().equals(resolved))
                        .filter(column -> column.getRelationship().isPresent())
                        .findAny()
                        .map(column -> accioMDL.getModel(column.getType()).orElseThrow(() -> new RuntimeException(format("Model %s not found", column.getType()))));
                relatedOptional
                        .ifPresent(relatedModel -> graph.addEdge(relatedModel, baseObject));
                return relatedOptional.orElse(context);
            }
            return context;
        }
    }

    private static Expression getFirst(Expression expression)
    {
        if (expression instanceof DereferenceExpression) {
            return getFirst(((DereferenceExpression) expression).getBase());
        }
        else if (expression instanceof SubscriptExpression) {
            return getFirst(((SubscriptExpression) expression).getBase());
        }
        return expression;
    }
}
