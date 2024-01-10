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
import io.accio.base.dto.Metric;
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
import java.util.NoSuchElementException;
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
    public static final AccioDataLineage EMPTY = new AccioDataLineage(AccioMDL.EMPTY);
    private final AccioMDL mdl;
    // key: column name, value: source columns name. format in QualifiedName is modelName.columnName
    private final Map<QualifiedName, Set<QualifiedName>> sourceColumnsMap;
    private final Map<QualifiedName, List<Vertex>> requiredFields;

    public static AccioDataLineage analyze(AccioMDL mdl)
    {
        return new AccioDataLineage(mdl);
    }

    private AccioDataLineage(AccioMDL mdl)
    {
        this.mdl = requireNonNull(mdl);
        this.sourceColumnsMap = collectSourceColumns();
        this.requiredFields = collectRequiredFieldsByColumn();
    }

    @VisibleForTesting
    LinkedHashMap<String, Set<String>> getRequiredFields(QualifiedName columnNames)
    {
        return getRequiredFields(ImmutableList.of(columnNames));
    }

    public LinkedHashMap<String, Set<String>> getRequiredFields(List<QualifiedName> columnNames)
    {
        // make sure there is no model dependency cycle in given columnNames.
        DirectedAcyclicGraph<Vertex, Object> graph = new DirectedAcyclicGraph<>(Object.class);
        Map<String, Vertex> vertexes = new HashMap<>();
        requiredFields.entrySet().stream()
                .filter(e -> columnNames.contains(e.getKey()))
                .forEach(e -> {
                    List<Vertex> nodes = ImmutableList.copyOf(e.getValue());
                    for (int i = 1; i < nodes.size(); i++) {
                        String from = nodes.get(i - 1).getName();
                        String to = nodes.get(i).getName();
                        Vertex vertexFrom = vertexes.computeIfAbsent(from, (ignored) -> new Vertex(from));
                        Vertex vertexTo = vertexes.computeIfAbsent(to, (ignored) -> new Vertex(to));
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
        graph.iterator().forEachRemaining(vertex -> result.put(vertex.getName(), vertex.getColumnNames()));
        // add back column names to requiredFields
        columnNames.forEach(fullColumnName -> {
            Set<String> names = Optional.ofNullable(result.get(getTable(fullColumnName))).orElseGet(HashSet::new);
            names.add(getColumn(fullColumnName));
            result.put(getTable(fullColumnName), names);
        });
        return result;
    }

    private Map<QualifiedName, Set<QualifiedName>> collectSourceColumns()
    {
        Map<QualifiedName, Set<QualifiedName>> sourceColumnsMap = new HashMap<>();
        for (Model model : mdl.listModels()) {
            for (Column column : model.getColumns()) {
                SetMultimap<String, String> sourceColumns = getSourceColumns(mdl, model, column);
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
        for (Metric metric : mdl.listMetrics()) {
            for (Column column : metric.getColumns()) {
                SetMultimap<String, String> sourceColumns = getSourceColumns(mdl, metric, column);
                sourceColumnsMap.put(
                        QualifiedName.of(metric.getName(), column.getName()),
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
        return sourceColumnsMap;
    }

    private Map<QualifiedName, List<Vertex>> collectRequiredFieldsByColumn()
    {
        Map<QualifiedName, List<Vertex>> columnLineages = new HashMap<>();
        sourceColumnsMap.forEach(((column, sourceColumns) -> {
            DirectedAcyclicGraph<Vertex, Object> graph = new DirectedAcyclicGraph<>(Object.class);
            Map<String, Vertex> vertexes = new HashMap<>();
            collectRequiredFields(column, graph, vertexes);
            columnLineages.put(column, ImmutableList.copyOf(graph.iterator()));
        }));

        return columnLineages;
    }

    private void collectRequiredFields(
            QualifiedName qualifiedName,
            DirectedAcyclicGraph<Vertex, Object> graph,
            Map<String, Vertex> vertexes)
    {
        Column targetColumn = mdl.getColumn(qualifiedName).orElseThrow(() -> new NoSuchElementException(qualifiedName + " not found"));
        String tableName = getTable(qualifiedName);
        String columnName = getColumn(qualifiedName);
        Vertex targetVertex = vertexes.computeIfAbsent(tableName, (ignored) -> new Vertex(tableName));
        graph.addVertex(targetVertex);
        if (sourceColumnsMap.containsKey(qualifiedName)) {
            Set<QualifiedName> sourceColumns = sourceColumnsMap.get(qualifiedName);
            // sometimes we can't analyze lineage from column expression e.g. count(*), while the column itself may depend on another object (e.g. metric/model...)
            // so we still need detect baseObject here.
            if (sourceColumns.isEmpty()) {
                Optional<String> baseObject = mdl.getBaseObject(tableName);
                if (baseObject.isPresent()) {
                    Vertex sourceVertex = vertexes.computeIfAbsent(baseObject.get(), (ignored) -> new Vertex(baseObject.get()));
                    mdl.getBaseObject(tableName).ifPresent(sourceTable -> graph.addVertex(sourceVertex));
                    try {
                        graph.addEdge(sourceVertex, targetVertex);
                    }
                    catch (GraphCycleProhibitedException ex) {
                        throw new IllegalArgumentException(format("found cycle: %s column: %s", tableName, columnName));
                    }
                }
            }
            else {
                sourceColumns.forEach(fullColumnName -> {
                    Column sourceColumn = mdl.getColumn(fullColumnName).orElseThrow(() -> new NoSuchElementException(fullColumnName + " not found"));
                    String sourceTableName = getTable(fullColumnName);
                    String sourceColumnName = getColumn(fullColumnName);
                    Vertex sourceVertex = vertexes.computeIfAbsent(sourceTableName, (ignored) -> new Vertex(sourceTableName));
                    graph.addVertex(sourceVertex);
                    if (!skipAddEdge(sourceVertex, sourceColumn, targetVertex, targetColumn)) {
                        try {
                            graph.addEdge(sourceVertex, targetVertex);
                        }
                        catch (GraphCycleProhibitedException ex) {
                            throw new IllegalArgumentException(format("found cycle: %s column: %s", tableName, columnName));
                        }
                    }
                    sourceVertex.columnNames.add(sourceColumnName);

                    // recursively create column lineage
                    collectRequiredFields(fullColumnName, graph, vertexes);
                });
            }
        }
    }

    private static boolean skipAddEdge(Vertex sourceVertex, Column sourceColumn, Vertex targetVertex, Column targetColumn)
    {
        // calculated field could be dependent on non-calculated field in the same model
        return sourceVertex.getName().equals(targetVertex.getName())
                && targetColumn.isCalculated()
                && !sourceColumn.isCalculated();
    }

    public static class Vertex
    {
        private final String name;
        private final Set<String> columnNames;

        public Vertex(String name)
        {
            this(name, new HashSet<>());
        }

        @VisibleForTesting
        public Vertex(String name, Set<String> columnNames)
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
     * @param column column to analyze source columns.
     * @return A SetMultimap which key is model name and value is a set of column names.
     */
    private static SetMultimap<String, String> getSourceColumns(AccioMDL mdl, Model model, Column column)
    {
        Expression expression;
        // TODO: current column expression allow not empty (i.e. its expression is the same as column name)
        //  we should not directly use dto object, instead, we should convert them into another object. and fill the expression in every column.
        if (column.getExpression().isEmpty()) {
            expression = parseExpression(column.getName());
        }
        else {
            expression = parseExpression(column.getExpression().get());
        }
        Analyzer analyzer = new Analyzer(mdl, model, column);
        analyzer.process(expression);
        return analyzer.getSourceColumns();
    }

    private static SetMultimap<String, String> getSourceColumns(AccioMDL mdl, Metric metric, Column column)
    {
        Expression expression;
        // TODO: current column expression allow not empty (i.e. its expression is the same as column name)
        //  we should not directly use dto object, instead, we should convert them into another object. and fill the expression in every column.
        if (column.getExpression().isEmpty()) {
            expression = parseExpression(column.getName());
        }
        else {
            expression = parseExpression(column.getExpression().get());
        }
        MetricAnalyzer analyzer = new MetricAnalyzer(mdl, metric, column);
        analyzer.process(expression);
        return analyzer.getSourceColumns();
    }

    private static class MetricAnalyzer
            extends DefaultTraversalVisitor<Void>
    {
        private final AccioMDL mdl;
        private final Metric metric;
        private final SetMultimap<String, String> sourceColumns = HashMultimap.create();

        private MetricAnalyzer(AccioMDL mdl, Metric metric, Column column)
        {
            this.mdl = requireNonNull(mdl);
            this.metric = requireNonNull(metric);
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            QualifiedName qualifiedName = getQualifiedName(node);
            if (qualifiedName == null) {
                return null;
            }
            Optional<Model> parentModel = mdl.getModel(metric.getBaseObject());
            if (parentModel.isPresent()) {
                Optional<ExpressionRelationshipInfo> relationshipInfo = createRelationshipInfo(qualifiedName, parentModel.get(), mdl).stream()
                        .filter(info -> info.getRelationships().size() > 0)
                        .filter(info -> info.getRemainingParts().size() > 0)
                        .findAny();
                relationshipInfo.ifPresent(info -> {
                    // collect join keys
                    for (int i = 0; i < info.getRelationships().size(); i++) {
                        Relationship relationship = info.getRelationships().get(i);
                        String left = relationship.getModels().get(0);
                        String right = relationship.getModels().get(1);
                        Expression joinCondition = parseExpression(relationship.getCondition());
                        String leftKey = getJoinKey(joinCondition, left).orElseThrow();
                        String rightKey = getJoinKey(joinCondition, right).orElseThrow();
                        sourceColumns.put(left, leftKey);
                        sourceColumns.put(right, rightKey);
                    }
                    // collect last relationship output column
                    Relationship relationship = info.getRelationships().get(info.getRelationships().size() - 1);
                    String columnName = info.getRemainingParts().get(info.getRemainingParts().size() - 1);
                    String modelName = relationship.getModels().get(1);
                    sourceColumns.put(modelName, columnName);
                });
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void ignored)
        {
            // TODO: exclude sql reserved words
            // handle metric on model
            Optional<Model> parentModel = mdl.getModel(metric.getBaseObject());
            if (parentModel.isPresent()) {
                parentModel.get().getColumns().stream()
                        .filter(column -> node.getValue().equals(column.getName()))
                        .findAny()
                        .ifPresent(column -> sourceColumns.put(parentModel.get().getName(), column.getName()));
                return null;
            }
            // handle metric on metric
            Optional<Metric> parentMetric = mdl.getMetric(metric.getBaseObject());
            if (parentMetric.isPresent()) {
                parentMetric.get().getColumns().stream()
                        .filter(column -> node.getValue().equals(column.getName()))
                        .findAny()
                        .ifPresent(column -> sourceColumns.put(parentMetric.get().getName(), column.getName()));
                return null;
            }
            return null;
        }

        public SetMultimap<String, String> getSourceColumns()
        {
            return sourceColumns;
        }
    }

    private static class Analyzer
            extends DefaultTraversalVisitor<Void>
    {
        private final AccioMDL mdl;
        private final Model model;
        private final Column column;
        private final SetMultimap<String, String> sourceColumns = HashMultimap.create();

        private Analyzer(AccioMDL mdl, Model model, Column column)
        {
            this.mdl = requireNonNull(mdl);
            this.model = requireNonNull(model);
            this.column = requireNonNull(column);
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            QualifiedName qualifiedName = getQualifiedName(node);
            if (qualifiedName == null) {
                return null;
            }
            if (column.isCalculated()) {
                Optional<ExpressionRelationshipInfo> relationshipInfo = createRelationshipInfo(qualifiedName, model, mdl).stream()
                        .filter(info -> info.getRelationships().size() > 0)
                        .filter(info -> info.getRemainingParts().size() > 0)
                        .findAny();
                relationshipInfo.ifPresent(info -> {
                    // collect join keys
                    for (int i = 0; i < info.getRelationships().size(); i++) {
                        Relationship relationship = info.getRelationships().get(i);
                        String left = relationship.getModels().get(0);
                        String right = relationship.getModels().get(1);
                        Expression joinCondition = parseExpression(relationship.getCondition());
                        String leftKey = getJoinKey(joinCondition, left).orElseThrow();
                        String rightKey = getJoinKey(joinCondition, right).orElseThrow();
                        sourceColumns.put(left, leftKey);
                        sourceColumns.put(right, rightKey);
                    }
                    // collect last relationship output column
                    Relationship relationship = info.getRelationships().get(info.getRelationships().size() - 1);
                    String columnName = info.getRemainingParts().get(info.getRemainingParts().size() - 1);
                    String modelName = relationship.getModels().get(1);
                    sourceColumns.put(modelName, columnName);
                });
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void ignored)
        {
            // TODO: exclude sql reserved words
            // if a column is calculated, it could only use non calculated columns defined in the current model.
            if (column.isCalculated()) {
                model.getColumns().stream()
                        .filter(column -> !column.isCalculated())
                        .filter(column -> node.getValue().equals(column.getName()))
                        .findAny()
                        .ifPresent(column -> sourceColumns.put(model.getName(), node.getValue()));
            }
            // handle non-calculated column
            else {
                // handle model on model
                if (model.getBaseObject() != null) {
                    Model parent = mdl.getModel(model.getBaseObject()).orElseThrow();
                    parent.getColumns().stream()
                            .filter(column -> node.getValue().equals(column.getName()))
                            .findAny()
                            .ifPresent(column -> sourceColumns.put(parent.getName(), column.getName()));
                }
            }
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

    public static String getTable(QualifiedName qualifiedName)
    {
        return qualifiedName.getParts().get(0);
    }

    public static String getColumn(QualifiedName qualifiedName)
    {
        return qualifiedName.getParts().get(1);
    }
}
