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

package io.accio.base.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static io.accio.base.dto.DateSpine.DEFAULT;
import static java.util.Objects.requireNonNull;

public class Manifest
{
    private final String catalog;
    private final String schema;
    private final DateSpine dateSpine;
    private final List<Model> models;
    private final List<Relationship> relationships;
    private final List<EnumDefinition> enumDefinitions;
    private final List<Metric> metrics;
    private final List<CumulativeMetric> cumulativeMetrics;

    private final List<View> views;

    public static Builder builder()
    {
        return new Builder();
    }

    @JsonCreator
    public Manifest(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("models") List<Model> models,
            @JsonProperty("relationships") List<Relationship> relationships,
            @JsonProperty("enumDefinitions") List<EnumDefinition> enumDefinitions,
            @JsonProperty("metrics") List<Metric> metrics,
            @JsonProperty("cumulativeMetrics") List<CumulativeMetric> cumulativeMetrics,
            @JsonProperty("views") List<View> views,
            @JsonProperty("dateSpine") DateSpine dateSpine)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.models = models == null ? List.of() : models;
        this.relationships = relationships == null ? List.of() : relationships;
        this.enumDefinitions = enumDefinitions == null ? List.of() : enumDefinitions;
        this.metrics = metrics == null ? List.of() : metrics;
        this.cumulativeMetrics = cumulativeMetrics == null ? List.of() : cumulativeMetrics;
        this.views = views == null ? List.of() : views;
        this.dateSpine = dateSpine == null ? DEFAULT : dateSpine;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public DateSpine getDateSpine()
    {
        return dateSpine;
    }

    @JsonProperty
    public List<Model> getModels()
    {
        return models;
    }

    @JsonProperty
    public List<Relationship> getRelationships()
    {
        return relationships;
    }

    @JsonProperty
    public List<EnumDefinition> getEnumDefinitions()
    {
        return enumDefinitions;
    }

    @JsonProperty
    public List<Metric> getMetrics()
    {
        return metrics;
    }

    @JsonProperty
    public List<CumulativeMetric> getCumulativeMetrics()
    {
        return cumulativeMetrics;
    }

    @JsonProperty
    public List<View> getViews()
    {
        return views;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Manifest manifest = (Manifest) o;
        return Objects.equals(catalog, manifest.catalog) &&
                Objects.equals(schema, manifest.schema) &&
                Objects.equals(models, manifest.models) &&
                Objects.equals(relationships, manifest.relationships) &&
                Objects.equals(enumDefinitions, manifest.enumDefinitions) &&
                Objects.equals(metrics, manifest.metrics) &&
                Objects.equals(cumulativeMetrics, manifest.cumulativeMetrics) &&
                Objects.equals(views, manifest.views) &&
                Objects.equals(dateSpine, manifest.dateSpine);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                catalog,
                schema,
                models,
                relationships,
                enumDefinitions,
                metrics,
                cumulativeMetrics,
                views,
                dateSpine);
    }

    @Override
    public String toString()
    {
        return "Manifest{" +
                "catalog='" + catalog + '\'' +
                ", schema='" + schema + '\'' +
                ", models=" + models +
                ", relationships=" + relationships +
                ", enumDefinitions=" + enumDefinitions +
                ", metrics=" + metrics +
                ", cumulativeMetrics=" + cumulativeMetrics +
                ", views=" + views +
                ", dateSpine=" + dateSpine +
                '}';
    }

    public static class Builder
    {
        private String catalog;
        private String schema;
        private List<Model> models;
        private List<Relationship> relationships;
        private List<EnumDefinition> enumDefinitions;
        private List<Metric> metrics;
        private List<CumulativeMetric> cumulativeMetrics;

        private List<View> views;

        private DateSpine dateSpine;

        private Builder() {}

        public Builder setCatalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public Builder setSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder setModels(List<Model> models)
        {
            this.models = models;
            return this;
        }

        public Builder setRelationships(List<Relationship> relationships)
        {
            this.relationships = relationships;
            return this;
        }

        public Builder setEnumDefinitions(List<EnumDefinition> enumDefinitions)
        {
            this.enumDefinitions = enumDefinitions;
            return this;
        }

        public Builder setMetrics(List<Metric> metrics)
        {
            this.metrics = metrics;
            return this;
        }

        public Builder setCumulativeMetrics(List<CumulativeMetric> cumulativeMetrics)
        {
            this.cumulativeMetrics = cumulativeMetrics;
            return this;
        }

        public Builder setViews(List<View> views)
        {
            this.views = views;
            return this;
        }

        public Builder setDateSpine(DateSpine dateSpine)
        {
            this.dateSpine = dateSpine;
            return this;
        }

        public Manifest build()
        {
            return new Manifest(catalog, schema, models, relationships, enumDefinitions, metrics, cumulativeMetrics, views, dateSpine);
        }
    }
}
