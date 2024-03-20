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
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.requireNonNullEmpty;
import static java.util.Objects.requireNonNull;

public class DateSpine
{
    public static final DateSpine DEFAULT = new DateSpine(TimeUnit.DAY, "1970-01-01", "2077-12-31", null);

    private final TimeUnit unit;
    private final String start;
    private final String end;
    private final Map<String, String> properties;

    @JsonCreator
    public DateSpine(
            @JsonProperty("unit") TimeUnit unit,
            @JsonProperty("start") String start,
            @JsonProperty("end") String end,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.unit = requireNonNull(unit, "unit is null");
        this.start = requireNonNullEmpty(start, "start is null or empty");
        this.end = requireNonNullEmpty(end, "end is null or empty");
        this.properties = properties == null ? ImmutableMap.of() : properties;
    }

    @JsonProperty
    public TimeUnit getUnit()
    {
        return unit;
    }

    @JsonProperty
    public String getStart()
    {
        return start;
    }

    @JsonProperty
    public String getEnd()
    {
        return end;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("unit", unit)
                .add("start", start)
                .add("end", end)
                .add("properties", properties)
                .toString();
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
        DateSpine dateSpine = (DateSpine) o;
        return unit == dateSpine.unit &&
                Objects.equals(start, dateSpine.start) &&
                Objects.equals(end, dateSpine.end) &&
                Objects.equals(properties, dateSpine.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unit, start, end, properties);
    }
}
