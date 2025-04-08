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

package io.wren.base.sqlrewrite;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;
import io.wren.base.Utils;
import io.wren.base.WrenMDL;
import io.wren.base.dto.Column;
import io.wren.base.dto.CumulativeMetric;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationship;
import io.wren.base.sqlrewrite.analyzer.ExpressionRelationshipAnalyzer;
import io.wren.base.sqlrewrite.analyzer.ExpressionRelationshipInfo;
import io.wren.base.sqlrewrite.analyzer.RelationshipColumnInfo;
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
import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

// TODO: take care view
public class WrenDataLineage
{
    public static final WrenDataLineage EMPTY = new WrenDataLineage(WrenMDL.EMPTY);
    private final WrenMDL mdl;
    // key: column name, value: source columns name. format in QualifiedName is modelName.columnName
    private final Map<QualifiedName, Set<QualifiedName>> sourceColumnsMap;
    private final Map<QualifiedName, List<Vertex>> requiredFields;

    public static WrenDataLineage analyze(WrenMDL mdl)
    {
        return new WrenDataLineage(mdl);
    }

    private WrenDataLineage(WrenMDL mdl)
    {
        this.mdl = requireNonNull(mdl);
        this.sourceColumnsMap = collectSourceColumns();
        this.requiredFields = collectRequiredFieldsByColumn();
    }

    /**
     * Retrieve source columns used in the given column. Note that this won't delve deep into the root
     * but will only return the columns utilized directly in the specified column.
     *
     * @param columnName QualifiedName that should only have two elements, first element is table name, second element is column name.
     * @return a map of source columns which map key is table name, and map value is column names.
     */
    public Map<String, Set<String>> getSourceColumns(QualifiedName columnName)
    {
        return Optional.ofNullable(sourceColumnsMap.get(columnName))
                .orElseGet(ImmutableSet::of)
                .stream()
                .collect(groupingBy(WrenDataLineage::getTable, mapping(WrenDataLineage::getColumn, toSet())));
    }

    @VisibleForTesting
    LinkedHashMap<String, Set<String>> getRequiredFields(QualifiedName columnName)
    {
        return getRequiredFields(ImmutableList.of(columnName));
    }

    /**
     * Retrieve tables in the order of CTE generation and their respective required columns for the given column.
     *
     * @param columnNames list of QualifiedName, the qualified name should only have two elements, first element is table name, second element is column name.
     * @return {@code LinkedHashMap<String, Set<String>>} which key is table name, value is a set of column names.
     * The order of the entry is the order of CTE generation. That's why use LinkedHashMap.
     */
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
        for (CumulativeMetric cumulativeMetric : mdl.listCumulativeMetrics()) {
            Utils.checkArgument(mdl.isObjectExist(cumulativeMetric.getBaseObject()), "cumulative metric base object %s not exist", cumulativeMetric.getBaseObject());
            // handle measure
            sourceColumnsMap.put(
                    QualifiedName.of(cumulativeMetric.getName(), cumulativeMetric.getMeasure().getName()),
                    ImmutableSet.of(QualifiedName.of(cumulativeMetric.getBaseObject(), cumulativeMetric.getMeasure().getRefColumn())));
            // handle window
            sourceColumnsMap.put(
                    QualifiedName.of(cumulativeMetric.getName(), cumulativeMetric.getWindow().getName()),
                    ImmutableSet.of(QualifiedName.of(cumulativeMetric.getBaseObject(), cumulativeMetric.getWindow().getRefColumn())));
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
        if (!sourceColumnsMap.containsKey(qualifiedName)) {
            return;
        }

        String targetTable = getTable(qualifiedName);
        String targetColumn = getColumn(qualifiedName);
        Set<QualifiedName> sourceColumns = sourceColumnsMap.get(qualifiedName);
        Vertex targetVertex = vertexes.computeIfAbsent(targetTable, (ignored) -> new Vertex(targetTable));
        graph.addVertex(targetVertex);
        // sometimes we can't analyze lineage from column expression e.g. count(*), while the column itself may depend on another object (e.g. metric/model...)
        // so we still need detect baseObject here.
        if (sourceColumns.isEmpty()) {
            String baseObject = getBaseObject(mdl, targetTable);
            if (mdl.isObjectExist(baseObject)) {
                Vertex sourceVertex = vertexes.computeIfAbsent(baseObject, (ignored) -> new Vertex(baseObject));
                graph.addVertex(sourceVertex);
                try {
                    graph.addEdge(sourceVertex, targetVertex);
                }
                catch (GraphCycleProhibitedException ex) {
                    throw new IllegalArgumentException(format("found cycle: %s column: %s", targetTable, targetColumn));
                }
            }
        }
        sourceColumns.forEach(fullSourceColumnName -> {
            String sourceTableName = getTable(fullSourceColumnName);
            String sourceColumnName = getColumn(fullSourceColumnName);
            Vertex sourceVertex = vertexes.computeIfAbsent(sourceTableName, (ignored) -> new Vertex(sourceTableName));
            graph.addVertex(sourceVertex);
            if (!skipAddEdge(fullSourceColumnName, qualifiedName)) {
                try {
                    graph.addEdge(sourceVertex, targetVertex);
                }
                catch (GraphCycleProhibitedException ex) {
                    throw new IllegalArgumentException(format("found cycle: %s column: %s", targetTable, targetColumn));
                }
            }
            sourceVertex.columnNames.add(sourceColumnName);
            // recursively create column lineage
            collectRequiredFields(fullSourceColumnName, graph, vertexes);
        });
    }

    private boolean skipAddEdge(QualifiedName sourceColumn, QualifiedName targetColumn)
    {
        // calculated field could be dependent on non-calculated field in the same model
        return getTable(sourceColumn).equals(getTable(targetColumn))
                && isCalculated(targetColumn)
                && !isCalculated(sourceColumn);
    }

    private boolean isCalculated(QualifiedName fullColumnName)
    {
        return mdl.getModel(getTable(fullColumnName))
                .map(Model::getColumns)
                .orElseGet(ImmutableList::of)
                .stream()
                .filter(column -> column.getName().equals(getColumn(fullColumnName)))
                .anyMatch(Column::isCalculated);
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
     * @param mdl WrenMDL
     * @param model the model that column expression belongs to.
     * @param column column to analyze source columns.
     * @return A SetMultimap which key is model name and value is a set of column names.
     */
    private static SetMultimap<String, String> getSourceColumns(WrenMDL mdl, Model model, Column column)
    {
        Expression expression;
        // TODO: current column expression allow not empty (i.e. its expression is the same as column name)
        //  we should not directly use dto object, instead, we should convert them into another object. and fill the expression in every column.
        expression = io.wren.base.sqlrewrite.Utils.parseExpression(column.getSqlExpression());
        Analyzer analyzer = new Analyzer(mdl, model, column);
        analyzer.process(expression);
        return analyzer.getSourceColumns();
    }

    private static SetMultimap<String, String> getSourceColumns(WrenMDL mdl, Metric metric, Column column)
    {
        Expression expression;
        // TODO: current column expression allow not empty (i.e. its expression is the same as column name)
        //  we should not directly use dto object, instead, we should convert them into another object. and fill the expression in every column.
        expression = io.wren.base.sqlrewrite.Utils.parseExpression(column.getSqlExpression());
        MetricAnalyzer analyzer = new MetricAnalyzer(mdl, metric, column);
        analyzer.process(expression);
        return analyzer.getSourceColumns();
    }

    private static class MetricAnalyzer
            extends DefaultTraversalVisitor<Void>
    {
        private final WrenMDL mdl;
        private final Metric metric;
        private final SetMultimap<String, String> sourceColumns = HashMultimap.create();

        private MetricAnalyzer(WrenMDL mdl, Metric metric, Column column)
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
                Optional<ExpressionRelationshipInfo> relationshipInfo = ExpressionRelationshipAnalyzer.createRelationshipInfo(qualifiedName, parentModel.get(), mdl).stream()
                        .filter(info -> info.getRelationships().size() > 0)
                        .filter(info -> info.getRemainingParts().size() > 0)
                        .findAny();
                relationshipInfo.ifPresent(info -> {
                    // collect relationship columns
                    for (RelationshipColumnInfo rsInfo : info.getRelationshipColumnInfos()) {
                        sourceColumns.put(rsInfo.getModel().getName(), rsInfo.getColumn().getName());
                    }
                    // collect last relationship output column
                    Relationship relationship = info.getRelationshipColumnInfos().get(info.getRelationshipColumnInfos().size() - 1).getNormalizedRelationship();
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
            if (isColumnExist(mdl, metric.getBaseObject(), node.getValue())) {
                sourceColumns.put(metric.getBaseObject(), node.getValue());
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
        private final WrenMDL mdl;
        private final Model model;
        private final Column column;
        private final SetMultimap<String, String> sourceColumns = HashMultimap.create();

        private Analyzer(WrenMDL mdl, Model model, Column column)
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
                Optional<ExpressionRelationshipInfo> relationshipInfo = ExpressionRelationshipAnalyzer.createRelationshipInfo(qualifiedName, model, mdl).stream()
                        .filter(info -> info.getRelationships().size() > 0)
                        .filter(info -> info.getRemainingParts().size() > 0)
                        .findAny();
                relationshipInfo.ifPresent(info -> {
                    // collect relationship columns
                    for (RelationshipColumnInfo rsInfo : info.getRelationshipColumnInfos()) {
                        sourceColumns.put(rsInfo.getModel().getName(), rsInfo.getColumn().getName());
                    }
                    // collect last relationship output column
                    Relationship relationship = info.getRelationshipColumnInfos().get(info.getRelationshipColumnInfos().size() - 1).getNormalizedRelationship();
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
                if (model.getBaseObject() == null) {
                    return null;
                }
                if (isColumnExist(mdl, model.getBaseObject(), node.getValue())) {
                    sourceColumns.put(model.getBaseObject(), node.getValue());
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

    private static boolean isColumnExist(WrenMDL mdl, String objectName, String columnName)
    {
        if (mdl.getModel(objectName).isPresent()) {
            return mdl.getModel(objectName).get().getColumns().stream()
                    .anyMatch(column -> columnName.equals(column.getName()));
        }
        else if (mdl.getMetric(objectName).isPresent()) {
            return mdl.getMetric(objectName).get().getColumns().stream()
                    .anyMatch(column -> columnName.equals(column.getName()));
        }
        else if (mdl.getCumulativeMetric(objectName).isPresent()) {
            CumulativeMetric cumulativeMetric = mdl.getCumulativeMetric(objectName).get();
            return cumulativeMetric.getMeasure().getName().equals(columnName)
                    || cumulativeMetric.getWindow().getName().equals(columnName);
        }
        return false;
    }

    private static String getBaseObject(WrenMDL mdl, String objectName)
    {
        if (mdl.getModel(objectName).isPresent()) {
            return mdl.getModel(objectName).get().getBaseObject();
        }
        else if (mdl.getMetric(objectName).isPresent()) {
            return mdl.getMetric(objectName).get().getBaseObject();
        }
        else if (mdl.getCumulativeMetric(objectName).isPresent()) {
            return mdl.getCumulativeMetric(objectName).get().getBaseObject();
        }
        return null;
    }
}
