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

package io.graphmdl.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.graphmdl.base.dto.EnumDefinition;
import io.graphmdl.base.dto.Manifest;
import io.graphmdl.base.dto.Metric;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.PreAggregationInfo;
import io.graphmdl.base.dto.Relationship;
import io.graphmdl.base.dto.View;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class GraphMDL
{
    public static final GraphMDL EMPTY_GRAPHMDL = GraphMDL.fromManifest(Manifest.builder().setCatalog("").setSchema("").build());

    private final String catalog;
    private final String schema;
    private final Manifest manifest;

    public static GraphMDL fromJson(String manifest)
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        return new GraphMDL(objectMapper.readValue(manifest, Manifest.class));
    }

    public static GraphMDL fromManifest(Manifest manifest)
    {
        return new GraphMDL(manifest);
    }

    private GraphMDL(Manifest manifest)
    {
        this.manifest = requireNonNull(manifest, "graphMDL is null");
        this.catalog = manifest.getCatalog();
        this.schema = manifest.getSchema();
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public List<Model> listModels()
    {
        return manifest.getModels();
    }

    public Optional<Model> getModel(String name)
    {
        return manifest.getModels().stream()
                .filter(model -> model.getName().equals(name))
                .findAny();
    }

    public List<Relationship> listRelationships()
    {
        return manifest.getRelationships();
    }

    public Optional<Relationship> getRelationship(String name)
    {
        return manifest.getRelationships().stream()
                .filter(relationship -> relationship.getName().equals(name))
                .findAny();
    }

    public List<EnumDefinition> listEnums()
    {
        return manifest.getEnumDefinitions();
    }

    public Optional<EnumDefinition> getEnum(String name)
    {
        return manifest.getEnumDefinitions().stream()
                .filter(enumField -> enumField.getName().equals(name))
                .findAny();
    }

    public List<Metric> listMetrics()
    {
        return manifest.getMetrics();
    }

    public List<PreAggregationInfo> listPreAggregated()
    {
        return manifest.getMetrics()
                .stream()
                .filter(Metric::isPreAggregated)
                .collect(toImmutableList());
    }

    public Optional<Metric> getMetric(String name)
    {
        return manifest.getMetrics().stream()
                .filter(metric -> metric.getName().equals(name))
                .findAny();
    }

    public Optional<Metric> getMetric(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return getMetric(name.getSchemaTableName().getTableName());
        }
        return Optional.empty();
    }

    public Optional<View> getView(String name)
    {
        return manifest.getViews().stream()
                .filter(view -> view.getName().equals(name))
                .findAny();
    }

    public Optional<View> getView(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return getView(name.getSchemaTableName().getTableName());
        }
        return Optional.empty();
    }
}
