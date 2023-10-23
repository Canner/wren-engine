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

package io.accio.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.accio.base.dto.Column;
import io.accio.base.dto.EnumDefinition;
import io.accio.base.dto.Manifest;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.base.dto.PreAggregationInfo;
import io.accio.base.dto.Relationship;
import io.accio.base.dto.View;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AccioMDL
{
    public static final AccioMDL EMPTY = AccioMDL.fromManifest(Manifest.builder().setCatalog("").setSchema("").build());
    private static final ObjectMapper MAPPER = new ObjectMapper().disable(FAIL_ON_UNKNOWN_PROPERTIES);

    private final String catalog;
    private final String schema;
    private final Manifest manifest;

    public static AccioMDL fromJson(String manifest)
            throws JsonProcessingException
    {
        return new AccioMDL(MAPPER.readValue(manifest, Manifest.class));
    }

    public static AccioMDL fromManifest(Manifest manifest)
    {
        return new AccioMDL(manifest);
    }

    private AccioMDL(Manifest manifest)
    {
        this.manifest = requireNonNull(manifest, "manifest is null");
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
        return Stream.concat(manifest.getMetrics().stream(), manifest.getModels().stream())
                .filter(PreAggregationInfo::isPreAggregated)
                .collect(toImmutableList());
    }

    public Optional<PreAggregationInfo> getPreAggregationInfo(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return listPreAggregated().stream()
                    .filter(preAggregationInfo -> preAggregationInfo.getName().equals(name.getSchemaTableName().getTableName()))
                    .findAny();
        }
        return Optional.empty();
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

    public static Optional<Column> getColumn(Model model, String name)
    {
        requireNonNull(model);
        requireNonNull(name);
        return model.getColumns().stream()
                .filter(column -> column.getName().equals(name))
                .findAny();
    }

    public static Optional<Column> getRelationshipColumn(Model model, String name)
    {
        return getColumn(model, name)
                .filter(column -> column.getRelationship().isPresent());
    }
}
