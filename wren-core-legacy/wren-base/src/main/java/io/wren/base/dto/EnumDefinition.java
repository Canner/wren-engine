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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.requireNonNullEmpty;
import static java.util.Objects.requireNonNull;

public class EnumDefinition
{
    public static EnumDefinition enumDefinition(String name, List<EnumValue> values)
    {
        return new EnumDefinition(name, values);
    }

    private final String name;
    private final List<EnumValue> values;

    @JsonCreator
    public EnumDefinition(
            @JsonProperty("name") String name,
            @JsonProperty("values") List<EnumValue> values)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        this.values = requireNonNull(values);
    }

    @JsonProperty
    public List<EnumValue> getValues()
    {
        return values;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public Optional<EnumValue> valueOf(String enumValueName)
    {
        return values.stream()
                .filter(candidate -> candidate.getName().equals(enumValueName))
                .findAny();
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
        EnumDefinition that = (EnumDefinition) obj;
        return Objects.equals(name, that.name) &&
                Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                name,
                values);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("values", values)
                .toString();
    }
}
