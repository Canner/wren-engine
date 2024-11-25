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
        return new EnumValue(name, value);
    }

    private final String name;
    private final String value;

    @JsonCreator
    public EnumValue(
            @JsonProperty("name") String name,
            @JsonProperty("value") String value)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        this.value = value;
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
                Objects.equals(value, enumValue.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("value", value)
                .toString();
    }
}
