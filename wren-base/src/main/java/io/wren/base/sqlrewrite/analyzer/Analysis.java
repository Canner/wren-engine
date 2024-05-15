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

package io.wren.base.sqlrewrite.analyzer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.dto.CumulativeMetric;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationship;
import io.wren.base.dto.View;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class Analysis
{
    private final Statement root;
    private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();

    private final Set<CatalogSchemaTableName> tables = new HashSet<>();
    private final Set<Relationship> relationships = new HashSet<>();
    private final Set<Model> models = new HashSet<>();
    private final Set<Metric> metrics = new HashSet<>();
    private final Map<NodeRef<FunctionRelation>, MetricRollupInfo> metricRollups = new HashMap<>();

    private final Set<CumulativeMetric> cumulativeMetrics = new HashSet<>();
    private final Set<View> views = new HashSet<>();
    private final Multimap<CatalogSchemaTableName, String> collectedColumns = HashMultimap.create();
    private final Map<NodeRef<Expression>, Field> referenceFields = new HashMap<>();

    private final Set<Node> requiredSourceNodes = new HashSet<>();

    private final Map<NodeRef<Node>, QualifiedName> sourceNodeNames = new HashMap<>();
    private final Map<NodeRef<Node>, Node> typeCoercionMap = new HashMap<>();

    public Analysis(Statement statement)
    {
        this.root = requireNonNull(statement, "statement is null");
    }

    public Statement getRoot()
    {
        return root;
    }

    void addTable(CatalogSchemaTableName tableName)
    {
        tables.add(tableName);
    }

    public Set<CatalogSchemaTableName> getTables()
    {
        return Set.copyOf(tables);
    }

    public Set<Relationship> getRelationships()
    {
        return relationships;
    }

    void addModels(Set<Model> models)
    {
        this.models.addAll(models);
    }

    public Set<Model> getModels()
    {
        return models;
    }

    void addMetrics(Set<Metric> metrics)
    {
        this.metrics.addAll(metrics);
    }

    public Set<Metric> getMetrics()
    {
        return metrics;
    }

    void addMetricRollups(NodeRef<FunctionRelation> metricRollupNodeRef, MetricRollupInfo metricRollupInfo)
    {
        metricRollups.put(metricRollupNodeRef, metricRollupInfo);
    }

    public Map<NodeRef<FunctionRelation>, MetricRollupInfo> getMetricRollups()
    {
        return metricRollups;
    }

    void addCumulativeMetrics(Set<CumulativeMetric> cumulativeMetrics)
    {
        this.cumulativeMetrics.addAll(cumulativeMetrics);
    }

    public Set<CumulativeMetric> getCumulativeMetrics()
    {
        return cumulativeMetrics;
    }

    public Set<View> getViews()
    {
        return views;
    }

    void addViews(Set<View> views)
    {
        this.views.addAll(views);
    }

    public Set<String> getWrenObjectNames()
    {
        return ImmutableSet.<String>builder()
                .addAll(getModels().stream().map(Model::getName).collect(toSet()))
                .addAll(getMetrics().stream().map(Metric::getName).collect(toSet()))
                .addAll(getCumulativeMetrics().stream().map(CumulativeMetric::getName).collect(toSet()))
                .addAll(getViews().stream().map(View::getName).collect(toSet()))
                .build();
    }

    void addCollectedColumns(List<Field> fields)
    {
        fields.forEach(field -> collectedColumns.put(field.getTableName(), field.getColumnName()));
    }

    public Multimap<CatalogSchemaTableName, String> getCollectedColumns()
    {
        return collectedColumns;
    }

    public void addReferenceFields(Map<NodeRef<Expression>, Field> referenceFields)
    {
        this.referenceFields.putAll(referenceFields);
    }

    public Map<NodeRef<Expression>, Field> getReferenceFields()
    {
        return referenceFields;
    }

    void addTypeCoercion(NodeRef<Node> nodeRef, Node node)
    {
        typeCoercionMap.put(nodeRef, node);
    }

    public Map<NodeRef<Node>, Node> getTypeCoercionMap()
    {
        return typeCoercionMap;
    }

    public Scope getScope(Node node)
    {
        return tryGetScope(node).orElseThrow(() -> new IllegalArgumentException(format("Analysis does not contain information for node: %s", node)));
    }

    public Optional<Scope> tryGetScope(Node node)
    {
        NodeRef<Node> key = NodeRef.of(node);
        if (scopes.containsKey(key)) {
            return Optional.of(scopes.get(key));
        }

        return Optional.empty();
    }

    public Scope getRootScope()
    {
        return getScope(root);
    }

    public void setScope(Node node, Scope scope)
    {
        scopes.put(NodeRef.of(node), scope);
    }

    public Map<NodeRef<Node>, Scope> getScopes()
    {
        return scopes;
    }

    public Set<Node> getRequiredSourceNodes()
    {
        return requiredSourceNodes;
    }

    public void addRequiredSourceNode(Node node)
    {
        requiredSourceNodes.add(node);
    }

    public Optional<QualifiedName> getSourceNodeNames(Node node)
    {
        return Optional.ofNullable(sourceNodeNames.get(NodeRef.of(node)));
    }

    public void addSourceNodeName(NodeRef<Node> nodeRef, QualifiedName name)
    {
        sourceNodeNames.put(nodeRef, name);
    }
}
