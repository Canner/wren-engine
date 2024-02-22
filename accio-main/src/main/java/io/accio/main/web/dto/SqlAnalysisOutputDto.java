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

package io.accio.main.web.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SqlAnalysisOutputDto
{
    private final String modelName;
    private final List<ColumnPredicateDto> columnPredicates;
    private final String limit;
    private final List<SortItem> ordering;

    @JsonCreator
    public SqlAnalysisOutputDto(
            @JsonProperty("modelName") String modelName,
            @JsonProperty("columnPredicates") List<ColumnPredicateDto> columnPredicates,
            @JsonProperty("limit") String limit,
            @JsonProperty("ordering") List<SortItem> ordering)
    {
        this.modelName = requireNonNull(modelName);
        this.columnPredicates = columnPredicates;
        this.limit = limit;
        this.ordering = ordering == null ? List.of() : ordering;
    }

    @JsonProperty
    public String getModelName()
    {
        return modelName;
    }

    @JsonProperty
    public List<ColumnPredicateDto> getColumnPredicates()
    {
        return columnPredicates;
    }

    @JsonProperty
    public String getLimit()
    {
        return limit;
    }

    @JsonProperty
    public List<SortItem> getOrdering()
    {
        return ordering;
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
        SqlAnalysisOutputDto that = (SqlAnalysisOutputDto) o;
        return Objects.equals(modelName, that.modelName) &&
                Objects.equals(columnPredicates, that.columnPredicates)
                && Objects.equals(limit, that.limit)
                && Objects.equals(ordering, that.ordering);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                modelName,
                columnPredicates,
                limit,
                ordering);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("modelName", modelName)
                .add("columnPredicates", columnPredicates)
                .toString();
    }

    public static class SortItem
    {
        private final String identifier;
        private final String direction;

        @JsonCreator
        public SortItem(
                @JsonProperty("identifier") String identifier,
                @JsonProperty("direction") String direction)
        {
            this.identifier = identifier;
            this.direction = direction;
        }

        @JsonProperty
        public String getIdentifier()
        {
            return identifier;
        }

        @JsonProperty
        public String getDirection()
        {
            return direction;
        }

        // generate to string function
        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("identifier", identifier)
                    .add("direction", direction)
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

            SortItem sortItem = (SortItem) o;
            return Objects.equals(identifier, sortItem.identifier) &&
                    Objects.equals(direction, sortItem.direction);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identifier, direction);
        }
    }
}
