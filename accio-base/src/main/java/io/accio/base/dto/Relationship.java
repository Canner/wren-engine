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

package io.accio.base.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static io.accio.base.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

public class Relationship
{
    private final String name;
    private final List<String> models;
    private final JoinType joinType;
    private final String condition;

    // for debugging internally
    private final boolean isReverse;
    private final List<SortKey> manySideSortKeys;
    private final String description;

    public static Relationship relationship(String name, List<String> models, JoinType joinType, String condition, List<SortKey> manySideSortKeys)
    {
        return relationship(name, models, joinType, condition, manySideSortKeys, null);
    }

    public static Relationship relationship(String name, List<String> models, JoinType joinType, String condition)
    {
        return relationship(name, models, joinType, condition, null, null);
    }

    public static Relationship relationship(String name, List<String> models, JoinType joinType, String condition, List<SortKey> manySideSortKeys, String description)
    {
        return new Relationship(name, models, joinType, condition, manySideSortKeys, description);
    }

    public static Relationship reverse(Relationship relationship)
    {
        return new Relationship(relationship.name, relationship.getModels(), JoinType.reverse(relationship.joinType), relationship.getCondition(), true, relationship.getManySideSortKeys(), relationship.getDescription());
    }

    @JsonCreator
    public Relationship(
            @JsonProperty("name") String name,
            @JsonProperty("models") List<String> models,
            @JsonProperty("joinType") JoinType joinType,
            @JsonProperty("condition") String condition,
            @JsonProperty("manySideSortKeys") List<SortKey> manySideSortKeys,
            @JsonProperty("description") String description)
    {
        this(name, models, joinType, condition, false, manySideSortKeys, description);
    }

    public Relationship(
            String name,
            List<String> models,
            JoinType joinType,
            String condition,
            boolean isReverse,
            List<SortKey> manySideSortKeys,
            String description)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(models != null && models.size() >= 2, "relationship should contain at least 2 models");
        this.models = models;
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.condition = requireNonNull(condition, "condition is null");
        this.isReverse = isReverse;
        this.manySideSortKeys = manySideSortKeys == null ? List.of() : manySideSortKeys;
        this.description = description;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<String> getModels()
    {
        return models;
    }

    @JsonProperty
    public JoinType getJoinType()
    {
        return joinType;
    }

    @JsonProperty
    public String getCondition()
    {
        return condition;
    }

    @JsonProperty
    public List<SortKey> getManySideSortKeys()
    {
        return manySideSortKeys;
    }

    public boolean isReverse()
    {
        return isReverse;
    }

    @JsonProperty
    public String getDescription()
    {
        return description;
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
        Relationship that = (Relationship) obj;
        return Objects.equals(name, that.name)
                && Objects.equals(models, that.models)
                && joinType == that.joinType
                && Objects.equals(condition, that.condition)
                && isReverse == that.isReverse
                && Objects.equals(manySideSortKeys, that.manySideSortKeys)
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, models, joinType, condition, isReverse, manySideSortKeys);
    }

    @Override
    public String toString()
    {
        return "Relationship{" +
                "name='" + name + '\'' +
                ", models=" + models +
                ", joinType=" + joinType +
                ", condition='" + condition + '\'' +
                ", isReverse=" + isReverse +
                ", manySideSortKeys=" + manySideSortKeys +
                ", description='" + description + '\'' +
                '}';
    }

    public static class SortKey
    {
        public enum Ordering
        {
            ASC,
            DESC;

            public static Ordering get(String value)
            {
                return Arrays.stream(values())
                        .filter(v -> v.toString().equalsIgnoreCase(value))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("Unsupported ordering"));
            }
        }

        private final String name;
        private final boolean isDescending;

        public static SortKey sortKey(String name, Ordering ordering)
        {
            return new SortKey(name, ordering);
        }

        @JsonCreator
        public SortKey(
                @JsonProperty("name") String name,
                @JsonProperty("ordering") Ordering ordering)
        {
            this.name = requireNonNull(name, "name is null");
            this.isDescending = ordering == Ordering.DESC;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public boolean isDescending()
        {
            return isDescending;
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
            SortKey sortKey = (SortKey) obj;
            return Objects.equals(name, sortKey.name)
                    && isDescending == sortKey.isDescending;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, isDescending);
        }

        @Override
        public String toString()
        {
            return "SortKey{" +
                    "name='" + name + '\'' +
                    ", isDescending=" + isDescending +
                    '}';
        }
    }
}
