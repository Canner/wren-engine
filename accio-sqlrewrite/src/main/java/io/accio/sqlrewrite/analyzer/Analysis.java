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

package io.accio.sqlrewrite.analyzer;

import io.accio.base.CatalogSchemaTableName;
import io.accio.base.dto.CumulativeMetric;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import io.accio.base.dto.View;
import io.trino.sql.tree.FunctionRelation;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class Analysis
{
    private final Statement root;
    private final Set<CatalogSchemaTableName> tables = new HashSet<>();
    private final Set<NodeRef<Table>> modelNodeRefs = new HashSet<>();
    private final Set<Relationship> relationships = new HashSet<>();
    private final Set<Model> models = new HashSet<>();
    private final Set<Metric> metrics = new HashSet<>();
    private final Map<NodeRef<FunctionRelation>, MetricRollupInfo> metricRollups = new HashMap<>();

    private final Set<CumulativeMetric> cumulativeMetrics = new HashSet<>();
    private final Set<View> views = new HashSet<>();

    Analysis(Statement statement)
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

    void addModelNodeRef(NodeRef<Table> tableNodeRef)
    {
        this.modelNodeRefs.add(tableNodeRef);
    }

    public Set<NodeRef<Table>> getModelNodeRefs()
    {
        return modelNodeRefs;
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
}
