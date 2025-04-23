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

package io.wren.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.hubspot.jinjava.Jinjava;
import io.wren.base.dto.CacheInfo;
import io.wren.base.dto.Column;
import io.wren.base.dto.CumulativeMetric;
import io.wren.base.dto.DateSpine;
import io.wren.base.dto.EnumDefinition;
import io.wren.base.dto.Manifest;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationable;
import io.wren.base.dto.Relationship;
import io.wren.base.dto.View;
import io.wren.base.jinjava.JinjavaExpressionProcessor;
import io.wren.base.jinjava.JinjavaUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.wren.base.macro.Parameter.TYPE.MACRO;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class WrenMDL
{
    public static final WrenMDL EMPTY = WrenMDL.fromManifest(Manifest.builder().setCatalog("default").setSchema("default").build());
    private static final ObjectMapper MAPPER = new ObjectMapper().disable(FAIL_ON_UNKNOWN_PROPERTIES);
    private static final Jinjava JINJAVA = new Jinjava();

    private final String catalog;
    private final String schema;
    private final Manifest manifest;
    private final Map<String, Model> models;
    private final Map<String, Metric> metrics;
    private final Map<String, CumulativeMetric> cumulativeMetrics;
    private final Map<String, Relationship> relationships;

    public static WrenMDL fromJson(String manifest)
            throws JsonProcessingException
    {
        return new WrenMDL(MAPPER.readValue(manifest, Manifest.class));
    }

    public static WrenMDL fromManifest(Manifest manifest)
    {
        return new WrenMDL(manifest);
    }

    private WrenMDL(Manifest manifest)
    {
        requireNonNull(manifest, "manifest is null");
        this.manifest = renderManifest(manifest);
        this.catalog = manifest.getCatalog();
        this.schema = manifest.getSchema();
        this.models = listModels().stream().collect(toImmutableMap(Model::getName, identity()));
        this.metrics = listMetrics().stream().collect(toImmutableMap(Metric::getName, identity()));
        this.cumulativeMetrics = listCumulativeMetrics().stream().collect(toImmutableMap(CumulativeMetric::getName, identity()));
        this.relationships = listRelationships().stream().collect(toImmutableMap(Relationship::getName, identity()));
    }

    private Manifest renderManifest(Manifest original)
    {
        String macroTags = original.getMacros().stream()
                .filter(macro -> macro.getParameters().stream().noneMatch(parameter -> parameter.getType() == MACRO))
                .map(JinjavaUtils::getMacroTag).collect(joining("\n"));
        List<Model> renderedModels = original.getModels().stream().map(model -> {
            List<io.wren.base.dto.Column> processed = model.getColumns().stream().map(column -> renderExpression(column, macroTags, original)).collect(toList());
            return new Model(
                    model.getName(),
                    model.getRefSql(),
                    model.getBaseObject(),
                    model.getTableReference(),
                    processed,
                    model.getPrimaryKey(),
                    model.isCached(),
                    model.getRefreshTime());
        }).collect(toList());

        List<Metric> renderedMetrics = original.getMetrics().stream().map(metric ->
                new Metric(metric.getName(),
                        metric.getBaseObject(),
                        metric.getDimension().stream().map(column -> renderExpression(column, macroTags, original)).collect(toList()),
                        metric.getMeasure().stream().map(column -> renderExpression(column, macroTags, original)).collect(toList()),
                        metric.getTimeGrain(),
                        metric.isCached(),
                        metric.getRefreshTime())
        ).collect(toList());

        return Manifest.builder(original)
                .setModels(renderedModels)
                .setMetrics(renderedMetrics)
                .build();
    }

    private io.wren.base.dto.Column renderExpression(io.wren.base.dto.Column original, String macroTags, Manifest unProcessedManifest)
    {
        if (original.getExpression().isEmpty()) {
            return original;
        }

        String withTag = macroTags + JinjavaExpressionProcessor.process(original.getSqlExpression(), unProcessedManifest.getMacros());
        String expression = JINJAVA.render(withTag, ImmutableMap.of());
        return new io.wren.base.dto.Column(original.getName(),
                original.getType(),
                original.getRelationship().orElse(null),
                original.isCalculated(),
                original.isNotNull(),
                expression);
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public Manifest getManifest()
    {
        return manifest;
    }

    public List<Model> listModels()
    {
        return manifest.getModels();
    }

    public Optional<Model> getModel(String name)
    {
        return Optional.ofNullable(models.get(name));
    }

    public List<Relationship> listRelationships()
    {
        return manifest.getRelationships();
    }

    public Optional<Relationship> getRelationship(String name)
    {
        return Optional.ofNullable(relationships.get(name));
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

    public List<CacheInfo> listCached()
    {
        return Stream.concat(manifest.getMetrics().stream(), manifest.getModels().stream())
                .filter(CacheInfo::isCached)
                .collect(toImmutableList());
    }

    public Optional<CacheInfo> getCacheInfo(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return listCached().stream()
                    .filter(cacheInfo -> cacheInfo.getName().equals(name.getSchemaTableName().getTableName()))
                    .findAny();
        }
        return Optional.empty();
    }

    public Optional<Metric> getMetric(String name)
    {
        return Optional.ofNullable(metrics.get(name));
    }

    public Optional<Metric> getMetric(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return getMetric(name.getSchemaTableName().getTableName());
        }
        return Optional.empty();
    }

    public List<CumulativeMetric> listCumulativeMetrics()
    {
        return manifest.getCumulativeMetrics();
    }

    public Optional<CumulativeMetric> getCumulativeMetric(String name)
    {
        return Optional.ofNullable(cumulativeMetrics.get(name));
    }

    public Optional<CumulativeMetric> getCumulativeMetric(CatalogSchemaTableName name)
    {
        if (catalog.equals(name.getCatalogName()) && schema.equals(name.getSchemaTableName().getSchemaName())) {
            return getCumulativeMetric(name.getSchemaTableName().getTableName());
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

    public List<View> listViews()
    {
        return manifest.getViews();
    }

    public static Optional<Column> getRelationshipColumn(Model model, String name)
    {
        return getColumn(model, name)
                .filter(column -> column.getRelationship().isPresent());
    }

    private static Optional<Column> getColumn(Model model, String name)
    {
        requireNonNull(model);
        requireNonNull(name);
        return model.getColumns().stream()
                .filter(column -> column.getName().equals(name))
                .findAny();
    }

    public Optional<String> getColumnType(String objectName, String columnName)
    {
        if (!isObjectExist(objectName)) {
            throw new IllegalArgumentException("Dataset " + objectName + " not found");
        }
        if (getModel(objectName).isPresent()) {
            return getModel(objectName).get().getColumns().stream()
                    .filter(column -> columnName.equals(column.getName()))
                    .map(io.wren.base.dto.Column::getType)
                    .findAny();
        }
        else if (getMetric(objectName).isPresent()) {
            return getMetric(objectName).get().getColumns().stream()
                    .filter(column -> columnName.equals(column.getName()))
                    .map(Column::getType)
                    .findAny();
        }
        else if (getCumulativeMetric(objectName).isPresent()) {
            CumulativeMetric cumulativeMetric = getCumulativeMetric(objectName).get();
            if (cumulativeMetric.getMeasure().getName().equals(columnName)) {
                return Optional.of(cumulativeMetric.getMeasure().getType());
            }
            if (cumulativeMetric.getWindow().getName().equals(columnName)) {
                return getColumnType(cumulativeMetric.getBaseObject(), cumulativeMetric.getWindow().getRefColumn());
            }
        }
        else if (getView(objectName).isPresent()) {
            return Optional.empty();
        }
        throw new IllegalArgumentException("Dataset " + objectName + " is not a model, metric, cumulative metric or view");
    }

    public DateSpine getDateSpine()
    {
        return manifest.getDateSpine();
    }

    public boolean isObjectExist(String name)
    {
        if (name == null) {
            return false;
        }
        return getModel(name).isPresent()
                || getMetric(name).isPresent()
                || getCumulativeMetric(name).isPresent()
                || getView(name).isPresent();
    }

    public Optional<Relationable> getRelationable(String name)
    {
        return getModel(name)
                .map(model -> (Relationable) model)
                .or(() -> getMetric(name).map(metric -> (Relationable) metric));
    }
}
