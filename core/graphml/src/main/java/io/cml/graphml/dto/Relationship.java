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

import static io.cml.graphml.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

public class Relationship
{
    private final String name;
    private final List<String> models;
    private final JoinType joinType;
    private final String condition;

    @JsonCreator
    public Relationship(
            @JsonProperty("name") String name,
            @JsonProperty("models") List<String> models,
            @JsonProperty("joinType") JoinType joinType,
            @JsonProperty("condition") String condition)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(models != null && models.size() >= 2, "relationship should contain at least 2 models");
        this.models = models;
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.condition = requireNonNull(condition, "condition is null");
    }

    public String getName()
    {
        return name;
    }

    public List<String> getModels()
    {
        return models;
    }

    public JoinType getJoinType()
    {
        return joinType;
    }

    public String getCondition()
    {
        return condition;
    }
}
