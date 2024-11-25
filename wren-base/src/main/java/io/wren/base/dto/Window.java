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
import io.wren.base.WrenTypes;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.requireNonNullEmpty;
import static java.util.Objects.requireNonNull;

public class Window
{
    private final String name;

    private final String refColumn;

    private final TimeUnit timeUnit;

    private final String start;
    private final String end;

    public static Window window(String name, String refColumn, TimeUnit timeUnit, String start, String end)
    {
        return new Window(name, refColumn, timeUnit, start, end);
    }

    @JsonCreator
    public Window(
            @JsonProperty("name") String name,
            @JsonProperty("refColumn") String refColumn,
            @JsonProperty("timeUnit") TimeUnit timeUnit,
            @JsonProperty("start") String start,
            @JsonProperty("end") String end)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        this.refColumn = requireNonNullEmpty(refColumn, "refColumn is null or empty");
        this.timeUnit = requireNonNull(timeUnit, "timeUnit is null or empty");
        this.start = requireNonNullEmpty(start, "start is null or empty");
        this.end = requireNonNullEmpty(end, "end is null or empty");
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
    public TimeUnit getTimeUnit()
    {
        return timeUnit;
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

    public Column toColumn()
    {
        return new Column(name, WrenTypes.TIMESTAMP, null, false, false, refColumn);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("refColumn", refColumn)
                .add("timeUnit", timeUnit)
                .add("start", start)
                .add("end", end)
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

        Window window = (Window) o;
        return Objects.equals(name, window.name) &&
                Objects.equals(refColumn, window.refColumn) &&
                timeUnit == window.timeUnit &&
                Objects.equals(start, window.start) &&
                Objects.equals(end, window.end);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, refColumn, timeUnit, start, end);
    }
}
