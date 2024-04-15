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

package io.wren.base.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.checkArgument;
import static io.wren.base.Utils.requireNonNullEmpty;
import static java.util.Objects.requireNonNull;

public class Metric
        implements CacheInfo, Relationable, Dataset
{
    private final String name;
    private final String baseObject;
    private final List<Column> dimension;
    private final List<Column> measure;
    private final List<TimeGrain> timeGrain;
    private final boolean cached;
    private final Duration refreshTime;
    private final Map<String, String> properties;

    public static Metric metric(String name, String baseObject, List<Column> dimension, List<Column> measure)
    {
        return metric(name, baseObject, dimension, measure, List.of(), false);
    }

    public static Metric metric(String name, String baseObject, List<Column> dimension, List<Column> measure, List<TimeGrain> timeGrain)
    {
        return metric(name, baseObject, dimension, measure, timeGrain, false);
    }

    public static Metric metric(String name, String baseObject, List<Column> dimension, List<Column> measure, List<TimeGrain> timeGrain, boolean cached)
    {
        return new Metric(name, baseObject, dimension, measure, timeGrain, cached, null, ImmutableMap.of());
    }

    @JsonCreator
    public Metric(
            @JsonProperty("name") String name,
            @JsonProperty("baseObject") String baseObject,
            @JsonProperty("dimension") List<Column> dimension,
            @JsonProperty("measure") List<Column> measure,
            @JsonProperty("timeGrain") List<TimeGrain> timeGrain,
            @JsonProperty("cached") boolean cached,
            @JsonProperty("refreshTime") Duration refreshTime,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        this.baseObject = requireNonNullEmpty(baseObject, "baseObject is null or empty");
        this.dimension = requireNonNull(dimension, "dimension is null");
        this.measure = requireNonNull(measure, "measure is null");
        this.cached = cached;
        checkArgument(!measure.isEmpty(), "the number of measures should be one at least");
        this.timeGrain = timeGrain == null ? ImmutableList.of() : timeGrain;
        this.refreshTime = refreshTime == null ? defaultRefreshTime : refreshTime;
        this.properties = properties == null ? ImmutableMap.of() : properties;
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    @JsonProperty
    public String getBaseObject()
    {
        return baseObject;
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
    public List<Column> getColumns()
    {
        return ImmutableList.<Column>builder().addAll(dimension).addAll(measure).build();
    }

    @Override
    @JsonProperty
    public boolean isCached()
    {
        return cached;
    }

    @Override
    @JsonProperty
    public Duration getRefreshTime()
    {
        return refreshTime;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
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
        return cached == that.cached &&
                Objects.equals(name, that.name) &&
                Objects.equals(baseObject, that.baseObject) &&
                Objects.equals(dimension, that.dimension) &&
                Objects.equals(measure, that.measure) &&
                Objects.equals(timeGrain, that.timeGrain) &&
                Objects.equals(refreshTime, that.refreshTime) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                name,
                baseObject,
                dimension,
                measure,
                timeGrain,
                cached,
                refreshTime,
                properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("baseObject", baseObject)
                .add("dimension", dimension)
                .add("measure", measure)
                .add("timeGrain", timeGrain)
                .add("cached", cached)
                .add("refreshTime", refreshTime)
                .add("properties", properties)
                .toString();
    }
}
