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
import io.airlift.units.Duration;

public class CumulativeMetric
{
    public static CumulativeMetric cumulativeMetric(
            String name,
            String baseModel,
            Column measure,
            Window window)
    {
        return new CumulativeMetric(name, baseModel, measure, window, false, null, null);
    }

    private final String name;
    private final String baseModel;
    private final Column measure;
    private final Window window;
    private final boolean preAggregated;
    private final Duration refreshTime;
    private final String description;

    @JsonCreator
    public CumulativeMetric(
            @JsonProperty("name") String name,
            @JsonProperty("baseModel") String baseModel,
            @JsonProperty("measure") Column measure,
            @JsonProperty("window") Window window,
            @JsonProperty("preAggregated") boolean preAggregated,
            @JsonProperty("refreshTime") Duration refreshTime,
            @JsonProperty("description") String description)
    {
        this.name = name;
        this.baseModel = baseModel;
        this.measure = measure;
        this.window = window;
        this.preAggregated = preAggregated;
        this.refreshTime = refreshTime;
        this.description = description;
    }

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
    public Column getMeasure()
    {
        return measure;
    }

    @JsonProperty
    public Window getWindow()
    {
        return window;
    }

    @JsonProperty
    public boolean isPreAggregated()
    {
        return preAggregated;
    }

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
}
