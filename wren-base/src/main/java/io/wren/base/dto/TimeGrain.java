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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.requireNonNullEmpty;

public class TimeGrain
{
    private final String name;

    private final String refColumn;
    private final List<TimeUnit> timeUnits;

    public static TimeGrain timeGrain(String name, String refColumn, List<TimeUnit> timeUnits)
    {
        return new TimeGrain(name, refColumn, timeUnits);
    }

    @JsonCreator
    public TimeGrain(
            @JsonProperty("name") String name,
            @JsonProperty("refColumn") String refColumn,
            @JsonProperty("dateParts") List<TimeUnit> timeUnits)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        this.refColumn = requireNonNullEmpty(refColumn, "refColumn is null or empty");
        this.timeUnits = timeUnits == null ? List.of() : timeUnits;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getRefColumn()
    {
        return refColumn;
    }

    @JsonProperty
    public List<TimeUnit> getDateParts()
    {
        return timeUnits;
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
        TimeGrain that = (TimeGrain) obj;
        return Objects.equals(name, that.name) &&
                Objects.equals(refColumn, that.refColumn) &&
                Objects.equals(timeUnits, that.timeUnits);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, refColumn, timeUnits);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("refColumn", refColumn)
                .add("timeUnits", timeUnits)
                .toString();
    }
}
