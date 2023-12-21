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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import io.accio.base.AccioMDL;
import io.accio.base.dto.Column;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import io.accio.sqlrewrite.analyzer.ExpressionRelationshipInfo;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.GraphCycleProhibitedException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.accio.sqlrewrite.Utils.parseExpression;
import static io.accio.sqlrewrite.analyzer.ExpressionRelationshipAnalyzer.createRelationshipInfo;
import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

// TODO: take care metric/cumulative metric/view, and model on model/metric, metric on model/metric etc.
public class AccioDataLineage
{
    private final AccioMDL mdl;
    // we only handle calculated field lineage here.
    private final Map<QualifiedName, DirectedAcyclicGraph<ModelVertex, Object>> requiredFieldsByColumn;
    // key: column name, value: source columns name. format in QualifiedName is modelName.columnName
    private final Map<QualifiedName, Set<QualifiedName>> sourceColumnsMap;

    public static AccioDataLineage analyze(AccioMDL mdl)
    {
        return new AccioDataLineage(mdl);
    }

    private AccioDataLineage(AccioMDL mdl)
    {
        this.mdl = requireNonNull(mdl);
        this.sourceColumnsMap = collectSourceColumns();
        this.requiredFieldsByColumn = collectRequiredFieldsByColumn();
    }

    @VisibleForTesting
    LinkedHashMap<String, Set<String>> getRequiredFields(QualifiedName columnNames)
    {
        return getRequiredFields(ImmutableList.of(columnNames));
    }

    public LinkedHashMap<String, Set<String>> getRequiredFields(List<QualifiedName> columnNames)
    {
        // make sure there is no model dependency cycle in given columnNames.
        DirectedAcyclicGraph<ModelVertex, Object> graph = new DirectedAcyclicGraph<>(Object.class);
        Map<String, ModelVertex> vertexes = new HashMap<>();
        requiredFieldsByColumn.entrySet().stream()
                .filter(e -> columnNames.contains(e.getKey()))
                .forEach(e -> {
                    List<ModelVertex> nodes = ImmutableList.copyOf(e.getValue().iterator());
                    for (int i = 1; i < nodes.size(); i++) {
                        String from = nodes.get(i - 1).getName();
                        String to = nodes.get(i).getName();
                        ModelVertex vertexFrom = vertexes.computeIfAbsent(from, (ignored) -> new ModelVertex(from));
                        ModelVertex vertexTo = vertexes.computeIfAbsent(to, (ignored) -> new ModelVertex(to));
                        graph.addVertex(vertexFrom);
                        graph.addVertex(vertexTo);
                        try {
                            graph.addEdge(vertexFrom, vertexTo);
                        }
                        catch (GraphCycleProhibitedException ex) {
                            throw new IllegalArgumentException("found cycle in " + e.getKey());
                        }
                        vertexFrom.columnNames.addAll(nodes.get(i - 1).getColumnNames());
                        vertexTo.columnNames.addAll(nodes.get(i).getColumnNames());
                    }
                });

        LinkedHashMap<String, Set<String>> result = new LinkedHashMap<>();
        graph.iterator().forEachRemaining(vertex -> {
            result.put(vertex.getName(), vertex.getColumnNames());
        });
        return result;
    }

    private Map<QualifiedName, Set<QualifiedName>> collectSourceColumns()
    {
        Map<QualifiedName, Set<QualifiedName>> sourceColumnsMap = new HashMap<>();
        for (Model model : mdl.listModels()) {
            for (Column column : model.getColumns()) {
                if (column.isCalculated() && column.getExpression().isPresent()) {
                    Expression expression = parseExpression(column.getExpression().get());
                    SetMultimap<String, String> sourceColumns = getSourceColumns(mdl, model, expression);
                    sourceColumnsMap.put(
                            QualifiedName.of(model.getName(), column.getName()),
                            // TODO: maybe we can make getSourceColumns return Set<QualifiedName>
                            sourceColumns.asMap().entrySet().stream()
                                    .map(e ->
                                            e.getValue().stream()
                                                    .map(name -> QualifiedName.of(e.getKey(), name))
                                                    .collect(toImmutableSet()))
                                    .flatMap(Set::stream)
                                    .collect(toImmutableSet()));
                }
            }
        }
        return sourceColumnsMap;
    }

    private Map<QualifiedName, DirectedAcyclicGraph<ModelVertex, Object>> collectRequiredFieldsByColumn()
    {
        Map<QualifiedName, DirectedAcyclicGraph<ModelVertex, Object>> columnLineages = new HashMap<>();
        sourceColumnsMap.forEach(((column, sourceColumns) -> {
            DirectedAcyclicGraph<ModelVertex, Object> graph = new DirectedAcyclicGraph<>(Object.class);
            Map<String, ModelVertex> vertexes = new HashMap<>();
            collectRequiredFields(column, graph, vertexes);
            columnLineages.put(column, graph);
        }));

        return columnLineages;
    }

    private void collectRequiredFields(
            QualifiedName qualifiedName,
            DirectedAcyclicGraph<ModelVertex, Object> graph,
            Map<String, ModelVertex> vertexes)
    {
        String modelName = getModel(qualifiedName);
        String columnName = getColumn(qualifiedName);
        ModelVertex targetVertex = vertexes.computeIfAbsent(modelName, (ignored) -> new ModelVertex(modelName));
        graph.addVertex(targetVertex);
        targetVertex.columnNames.add(columnName);
        if (sourceColumnsMap.containsKey(qualifiedName)) {
            Set<QualifiedName> sourceColumns = sourceColumnsMap.get(qualifiedName);
            sourceColumns.forEach(fullColumnName -> {
                String sourceModelName = getModel(fullColumnName);
                String sourceColumnName = getColumn(fullColumnName);
                ModelVertex sourceVertex = vertexes.computeIfAbsent(sourceModelName, (ignored) -> new ModelVertex(sourceModelName));
                graph.addVertex(sourceVertex);
                try {
                    graph.addEdge(sourceVertex, targetVertex);
                }
                catch (GraphCycleProhibitedException ex) {
                    throw new IllegalArgumentException(format("found cycle in model: %s column: %s", modelName, columnName));
                }
                sourceVertex.columnNames.add(sourceColumnName);

                // recursively create column lineage
                collectRequiredFields(fullColumnName, graph, vertexes);
            });
        }
    }

    public static class ModelVertex
    {
        private final String name;
        private final Set<String> columnNames;

        public ModelVertex(String name)
        {
            this(name, new HashSet<>());
        }

        @VisibleForTesting
        public ModelVertex(String name, Set<String> columnNames)
        {
            this.name = requireNonNull(name);
            this.columnNames = requireNonNull(columnNames);
        }

        public String getName()
        {
            return name;
        }

        public Set<String> getColumnNames()
        {
            return columnNames;
        }
    }

    /**
     * Collect the required source columns in given expression. Note that this method will only trace
     * the columns currently utilized in the expression, without further tracking to identify the columns required
     * behind those used in the expression.
     *
     * @param mdl AccioMDL
     * @param model the model that column expression belongs to.
     * @param expression expression in the column
     * @return A SetMultimap which key is model name and value is a set of column names.
     */
    private static SetMultimap<String, String> getSourceColumns(AccioMDL mdl, Model model, Expression expression)
    {
        Analyzer analyzer = new Analyzer(mdl, model);
        analyzer.process(expression);
        return analyzer.getSourceColumns();
    }

    private static class Analyzer
            extends DefaultTraversalVisitor<Void>
    {
        private final AccioMDL mdl;
        private final Model model;
        private final SetMultimap<String, String> sourceColumns = HashMultimap.create();

        private Analyzer(AccioMDL mdl, Model model)
        {
            this.mdl = requireNonNull(mdl);
            this.model = requireNonNull(model);
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            QualifiedName qualifiedName = getQualifiedName(node);
            if (qualifiedName != null) {
                Optional<ExpressionRelationshipInfo> relationshipInfo = createRelationshipInfo(qualifiedName, model, mdl).stream()
                        .filter(info -> info.getRelationships().size() > 0)
                        .filter(info -> info.getRemainingParts().size() > 0)
                        .findAny();
                relationshipInfo.ifPresent(info -> {
                    // collect join keys
                    for (int i = 0; i < info.getRelationships().size(); i++) {
                        Relationship relationship = info.getRelationships().get(i);
                        String modelName = relationship.getModels().get(1);
                        String joinKey = getJoinKey(parseExpression(relationship.getCondition()), modelName).orElseThrow();
                        sourceColumns.put(modelName, joinKey);
                    }
                    // collect last relationship output column
                    Relationship relationship = info.getRelationships().get(info.getRelationships().size() - 1);
                    String columnName = info.getRemainingParts().get(info.getRemainingParts().size() - 1);
                    sourceColumns.put(relationship.getModels().get(1), columnName);
                });
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void ignored)
        {
            // TODO: exclude sql reserved words
            sourceColumns.put(model.getName(), node.getValue());
            return null;
        }

        public SetMultimap<String, String> getSourceColumns()
        {
            return sourceColumns;
        }
    }

    public static Optional<String> getJoinKey(Expression expression, String modelName)
    {
        JoinKey joinKey = new JoinKey();
        joinKey.process(expression);
        return Optional.ofNullable(joinKey.getJoinKeys().get(modelName));
    }

    private static class JoinKey
            extends DefaultTraversalVisitor<Void>
    {
        private final Map<String, String> joinKeys = new HashMap<>();

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);
            if (qualifiedName != null && qualifiedName.getParts().size() == 2) {
                joinKeys.put(qualifiedName.getParts().get(0), qualifiedName.getParts().get(1));
            }
            return null;
        }

        public Map<String, String> getJoinKeys()
        {
            return joinKeys;
        }
    }

    private static String getModel(QualifiedName qualifiedName)
    {
        return qualifiedName.getParts().get(0);
    }

    private static String getColumn(QualifiedName qualifiedName)
    {
        return qualifiedName.getParts().get(1);
    }
}
