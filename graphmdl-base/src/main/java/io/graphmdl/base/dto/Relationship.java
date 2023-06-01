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

package io.graphmdl.base.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static io.graphmdl.base.Utils.checkArgument;
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

    public static Relationship relationship(String name, List<String> models, JoinType joinType, String condition, List<SortKey> manySideSortKeys)
    {
        return new Relationship(name, models, joinType, condition, false, manySideSortKeys);
    }

    public static Relationship relationship(String name, List<String> models, JoinType joinType, String condition)
    {
        return new Relationship(name, models, joinType, condition, false, null);
    }

    public static Relationship reverse(Relationship relationship)
    {
        return new Relationship(relationship.name, relationship.getModels(), JoinType.reverse(relationship.joinType), relationship.getCondition(), true, relationship.getManySideSortKeys());
    }

    @JsonCreator
    public Relationship(
            @JsonProperty("name") String name,
            @JsonProperty("models") List<String> models,
            @JsonProperty("joinType") JoinType joinType,
            @JsonProperty("condition") String condition,
            @JsonProperty("manySideSortKeys") List<SortKey> manySideSortKeys)
    {
        this(name, models, joinType, condition, false, manySideSortKeys);
    }

    public Relationship(
            String name,
            List<String> models,
            JoinType joinType,
            String condition,
            boolean isReverse,
            List<SortKey> manySideSortKeys)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(models != null && models.size() >= 2, "relationship should contain at least 2 models");
        this.models = models;
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.condition = requireNonNull(condition, "condition is null");
        this.isReverse = isReverse;
        this.manySideSortKeys = manySideSortKeys == null ? List.of() : manySideSortKeys;
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

    @JsonProperty
    public boolean isReverse()
    {
        return isReverse;
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
                && Objects.equals(manySideSortKeys, that.manySideSortKeys);
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
                '}';
    }

    public static class SortKey
    {
        public enum Ordering
        {
            ASC,
            DESC
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
