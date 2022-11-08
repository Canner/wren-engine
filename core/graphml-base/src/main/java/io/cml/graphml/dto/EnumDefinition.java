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

package io.cml.graphml.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

public class EnumDefinition
{
    public static EnumDefinition enumDefinition(String name, List<String> values)
    {
        return new EnumDefinition(name, values);
    }

    private final String name;
    private final List<String> values;

    @JsonCreator
    public EnumDefinition(
            @JsonProperty("name") String name,
            @JsonProperty("values") List<String> values)
    {
        this.name = name;
        this.values = values;
    }

    public List<String> getValues()
    {
        return values;
    }

    public String getName()
    {
        return name;
    }

    public Optional<String> valueOf(String value)
    {
        return values.stream()
                .filter(candidate -> candidate.equals(value))
                .findAny();
    }
}
