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

package io.graphmdl.base.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.graphmdl.base.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

public class Metric
        implements PreAggregationInfo
{
    private final String name;
    private final String baseModel;
    private final List<Column> dimension;
    private final List<Column> measure;
    private final List<TimeGrain> timeGrain;
    private final boolean preAggregated;
    private final Duration refreshTime;
    private final String description;

    public static Metric metric(String name, String baseModel, List<Column> dimension, List<Column> measure, List<TimeGrain> timeGrain)
    {
        return metric(name, baseModel, dimension, measure, timeGrain, false);
    }

    public static Metric metric(String name, String baseModel, List<Column> dimension, List<Column> measure, List<TimeGrain> timeGrain, boolean preAggregated)
    {
        return metric(name, baseModel, dimension, measure, timeGrain, preAggregated, null);
    }

    public static Metric metric(String name, String baseModel, List<Column> dimension, List<Column> measure, List<TimeGrain> timeGrain, boolean preAggregated, String description)
    {
        return new Metric(name, baseModel, dimension, measure, timeGrain, preAggregated, null, description);
    }

    @JsonCreator
    public Metric(
            @JsonProperty("name") String name,
            @JsonProperty("baseModel") String baseModel,
            @JsonProperty("dimension") List<Column> dimension,
            @JsonProperty("measure") List<Column> measure,
            @JsonProperty("timeGrain") List<TimeGrain> timeGrain,
            @JsonProperty("preAggregated") boolean preAggregated,
            @JsonProperty("refreshTime") Duration refreshTime,
            @JsonProperty("description") String description)
    {
        this.name = requireNonNull(name, "name is null");
        this.baseModel = requireNonNull(baseModel, "baseModel is null");
        this.dimension = requireNonNull(dimension, "dimension is null");
        this.measure = requireNonNull(measure, "measure is null");
        this.preAggregated = preAggregated;
        checkArgument(measure.size() > 0, "the number of measures should be one at least");
        this.timeGrain = requireNonNull(timeGrain, "timeGrain is null");
        this.refreshTime = refreshTime == null ? defaultRefreshTime : refreshTime;
        this.description = description;
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getBaseModel()
    {
        return baseModel;
    }

    @JsonProperty
    public List<Column> getDimension()
    {
        return dimension;
    }

    @JsonProperty
    public List<Column> getMeasure()
    {
        return measure;
    }

    @JsonProperty
    public List<TimeGrain> getTimeGrain()
    {
        return timeGrain;
    }

    public Optional<TimeGrain> getTimeGrain(String timeGrainName)
    {
        return timeGrain.stream()
                .filter(timeGrain -> timeGrain.getName().equals(timeGrainName))
                .findFirst();
    }

    @Override
    @JsonProperty
    public boolean isPreAggregated()
    {
        return preAggregated;
    }

    @Override
    @JsonProperty
    public Duration getRefreshTime()
    {
        return refreshTime;
    }

    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Metric that = (Metric) obj;
        return preAggregated == that.preAggregated
                && Objects.equals(name, that.name)
                && Objects.equals(baseModel, that.baseModel)
                && Objects.equals(dimension, that.dimension)
                && Objects.equals(measure, that.measure)
                && Objects.equals(timeGrain, that.timeGrain)
                && Objects.equals(refreshTime, that.refreshTime)
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                name,
                baseModel,
                dimension,
                measure,
                timeGrain,
                preAggregated,
                refreshTime,
                description);
    }

    @Override
    public String toString()
    {
        return "Metric{" +
                "name='" + name + '\'' +
                ", baseModel='" + baseModel + '\'' +
                ", dimension=" + dimension +
                ", measure=" + measure +
                ", timeGrain=" + timeGrain +
                ", preAggregated=" + preAggregated +
                ", refreshTime=" + refreshTime +
                ", description='" + description + '\'' +
                '}';
    }
}
