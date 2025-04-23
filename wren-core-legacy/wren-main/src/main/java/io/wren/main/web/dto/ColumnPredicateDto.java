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

package io.wren.main.web.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ColumnPredicateDto
{
    private final String columnName;
    private final List<PredicateDto> predicates;

    @JsonCreator
    public ColumnPredicateDto(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("predicates") List<PredicateDto> predicates)
    {
        this.columnName = requireNonNull(columnName);
        this.predicates = requireNonNull(predicates);
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public List<PredicateDto> getPredicates()
    {
        return predicates;
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
        ColumnPredicateDto that = (ColumnPredicateDto) o;
        return Objects.equals(columnName, that.columnName) && Objects.equals(predicates, that.predicates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, predicates);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("predicates", predicates)
                .toString();
    }
}
