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

package io.graphmdl.sqlrewrite.analyzer;

import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.dto.Metric;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.Relationship;
import io.graphmdl.sqlrewrite.RelationshipCteGenerator;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Analysis
{
    private final Statement root;
    private final Set<CatalogSchemaTableName> tables = new HashSet<>();
    private final RelationshipCteGenerator relationshipCteGenerator;
    private final Map<NodeRef<Expression>, Expression> relationshipFields = new HashMap<>();
    private final Set<NodeRef<Table>> modelNodeRefs = new HashSet<>();
    private final Map<NodeRef<Relation>, Set<String>> replaceTableWithCTEs = new HashMap<>();
    private final Set<Relationship> relationships = new HashSet<>();
    private final Set<Model> models = new HashSet<>();
    private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();
    private final Set<Metric> metrics = new HashSet<>();
    private final Map<NodeRef<FunctionRelation>, MetricRollupInfo> metricRollups = new HashMap<>();

    Analysis(Statement statement, RelationshipCteGenerator relationshipCteGenerator)
    {
        this.root = requireNonNull(statement, "statement is null");
        this.relationshipCteGenerator = relationshipCteGenerator;
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

    public Map<String, WithQuery> getRelationshipCTE()
    {
        return relationshipCteGenerator.getRegisteredCte();
    }

    public Map<String, String> getRelationshipNameMapping()
    {
        return relationshipCteGenerator.getNameMapping();
    }

    public Map<String, RelationshipCteGenerator.RelationshipCTEJoinInfo> getRelationshipInfoMapping()
    {
        return relationshipCteGenerator.getRelationshipInfoMapping();
    }

    void addRelationshipFields(Map<NodeRef<Expression>, Expression> relationshipFields)
    {
        this.relationshipFields.putAll(relationshipFields);
    }

    public Map<NodeRef<Expression>, Expression> getRelationshipFields()
    {
        return Map.copyOf(relationshipFields);
    }

    public void addReplaceTableWithCTEs(NodeRef<Relation> relationNodeRef, Set<String> relationshipCTENames)
    {
        this.replaceTableWithCTEs.put(relationNodeRef, relationshipCTENames);
    }

    public Map<NodeRef<Relation>, Set<String>> getReplaceTableWithCTEs()
    {
        return Map.copyOf(replaceTableWithCTEs);
    }

    void addRelationships(Set<Relationship> relationships)
    {
        this.relationships.addAll(relationships);
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

    void addModelNodeRef(NodeRef<Table> tableNodeRef)
    {
        this.modelNodeRefs.add(tableNodeRef);
    }

    public Set<NodeRef<Table>> getModelNodeRefs()
    {
        return modelNodeRefs;
    }

    public Optional<RelationType> getOutputDescriptor(Node node)
    {
        return getScope(node).getRelationType();
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

    void addMetrics(Set<Metric> metrics)
    {
        this.metrics.addAll(metrics);
    }

    public Set<Metric> getMetrics()
    {
        return metrics;
    }

    public void setScope(Node node, Scope scope)
    {
        scopes.put(NodeRef.of(node), scope);
    }

    void addMetricRollups(NodeRef<FunctionRelation> metricRollupNodeRef, MetricRollupInfo metricRollupInfo)
    {
        metricRollups.put(metricRollupNodeRef, metricRollupInfo);
    }

    public Map<NodeRef<FunctionRelation>, MetricRollupInfo> getMetricRollups()
    {
        return metricRollups;
    }
}
