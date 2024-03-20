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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.requireNonNullEmpty;

public class EnumValue
{
    public static EnumValue enumValue(String name)
    {
        return enumValue(name, null);
    }

    public static EnumValue enumValue(String name, String value)
    {
        return new EnumValue(name, value, null);
    }

    private final String name;
    private final String value;
    private final Map<String, String> properties;

    @JsonCreator
    public EnumValue(
            @JsonProperty("name") String name,
            @JsonProperty("value") String value,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        this.value = value;
        this.properties = properties == null ? ImmutableMap.of() : properties;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getValue()
    {
        return Optional.ofNullable(value).orElse(name);
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
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
        EnumValue enumValue = (EnumValue) o;
        return Objects.equals(name, enumValue.name) &&
                Objects.equals(value, enumValue.value) &&
                Objects.equals(properties, enumValue.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value, properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("value", value)
                .add("properties", properties)
                .toString();
    }
}
