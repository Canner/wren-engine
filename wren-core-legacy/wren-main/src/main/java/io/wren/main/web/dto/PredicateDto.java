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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PredicateDto
{
    private final String operator;
    private final String value;

    @JsonCreator
    public PredicateDto(
            @JsonProperty("operator") String operator,
            @JsonProperty("value") String value)
    {
        this.operator = requireNonNull(operator);
        this.value = requireNonNull(value);
    }

    @JsonProperty
    public String getOperator()
    {
        return operator;
    }

    @JsonProperty
    public String getValue()
    {
        return value;
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
        PredicateDto that = (PredicateDto) o;
        return Objects.equals(operator, that.operator) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("operator", operator)
                .add("value", value)
                .toString();
    }
}
