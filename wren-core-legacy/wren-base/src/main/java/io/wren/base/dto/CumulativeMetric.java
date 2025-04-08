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
import io.airlift.units.Duration;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.requireNonNullEmpty;
import static java.util.Objects.requireNonNull;

public class CumulativeMetric
        implements CacheInfo
{
    public static CumulativeMetric cumulativeMetric(
            String name,
            String baseObject,
            Measure measure,
            Window window)
    {
        return new CumulativeMetric(name, baseObject, measure, window, false, null);
    }

    private final String name;
    private final String baseObject;
    private final Measure measure;
    private final Window window;
    private final boolean cached;
    private final Duration refreshTime;

    @JsonCreator
    public CumulativeMetric(
            @JsonProperty("name") String name,
            @JsonProperty("baseObject") String baseObject,
            @JsonProperty("measure") Measure measure,
            @JsonProperty("window") Window window,
            @JsonProperty("cached") boolean cached,
            @JsonProperty("refreshTime") Duration refreshTime)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        this.baseObject = requireNonNullEmpty(baseObject, "baseObject is null or empty");
        this.measure = requireNonNull(measure, "measure is null");
        this.window = requireNonNull(window, "window is null");
        this.cached = cached;
        this.refreshTime = refreshTime;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getBaseObject()
    {
        return baseObject;
    }

    @JsonProperty
    public Measure getMeasure()
    {
        return measure;
    }

    @JsonProperty
    public Window getWindow()
    {
        return window;
    }

    @JsonProperty
    public boolean isCached()
    {
        return cached;
    }

    @JsonProperty
    public Duration getRefreshTime()
    {
        return refreshTime;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, baseObject, measure, window, cached, refreshTime);
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

        CumulativeMetric that = (CumulativeMetric) o;
        return cached == that.cached &&
                Objects.equals(name, that.name) &&
                Objects.equals(baseObject, that.baseObject) &&
                Objects.equals(measure, that.measure) &&
                Objects.equals(window, that.window) &&
                Objects.equals(refreshTime, that.refreshTime);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("baseObject", baseObject)
                .add("measure", measure)
                .add("window", window)
                .add("cached", cached)
                .add("refreshTime", refreshTime)
                .toString();
    }
}
