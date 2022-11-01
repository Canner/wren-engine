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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Column
{
    private final String name;
    private final Type type;
    private final Optional<String> relationship;

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("relationship") String relationship)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.relationship = Optional.ofNullable(relationship);
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public Optional<String> getRelationship()
    {
        return relationship;
    }
}
