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

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Column
{
    private final String name;
    private final String type;

    private final boolean notNull;
    private final String relationship;
    private final String expression;

    public static Column column(String name, String type, String relationship, boolean notNull)
    {
        return new Column(name, type, relationship, notNull, null);
    }

    public static Column column(String name, String type, String relationship, boolean notNull, String expression)
    {
        return new Column(name, type, relationship, notNull, expression);
    }

    public static Column relationshipColumn(String name, String type, String relationship)
    {
        return new Column(name, type, relationship, false, null);
    }

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("relationship") String relationship,
            @JsonProperty("notNull") boolean notNull,
            @JsonProperty("expression") String expression)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.relationship = relationship;
        this.notNull = notNull;
        this.expression = expression;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public Optional<String> getRelationship()
    {
        return Optional.ofNullable(relationship);
    }

    public boolean isNotNull()
    {
        return notNull;
    }

    public Optional<String> getExpression()
    {
        return Optional.ofNullable(expression);
    }

    public String getSqlExpression()
    {
        if (getRelationship().isPresent()) {
            return String.format("'relationship<%s>' as %s", relationship, name);
        }

        if (getExpression().isEmpty()) {
            return getName();
        }

        return String.format("%s as %s", expression, name);
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
        Column that = (Column) obj;
        return notNull == that.notNull
                && Objects.equals(name, that.name)
                && Objects.equals(type, that.type)
                && Objects.equals(relationship, that.relationship)
                && Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, notNull, relationship, expression);
    }
}
