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
    private final String description;

    public static Column column(String name, String type, String relationship, boolean notNull)
    {
        return column(name, type, relationship, notNull, null, null);
    }

    public static Column column(String name, String type, String relationship, boolean notNull, String expression)
    {
        return column(name, type, relationship, notNull, expression, null);
    }

    public static Column column(String name, String type, String relationship, boolean notNull, String expression, String description)
    {
        return new Column(name, type, relationship, notNull, expression, description);
    }

    public static Column relationshipColumn(String name, String type, String relationship)
    {
        return new Column(name, type, relationship, false, null, null);
    }

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("relationship") String relationship,
            @JsonProperty("notNull") boolean notNull,
            @JsonProperty("expression") String expression,
            @JsonProperty("description") String description)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.relationship = relationship;
        this.notNull = notNull;
        this.expression = expression;
        this.description = description;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getRelationship()
    {
        return Optional.ofNullable(relationship);
    }

    @JsonProperty
    public boolean isNotNull()
    {
        return notNull;
    }

    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    @JsonProperty
    public Optional<String> getExpression()
    {
        return Optional.ofNullable(expression);
    }

    public String getSqlExpression()
    {
        if (getExpression().isEmpty()) {
            return quote(name);
        }

        return String.format("%s as %s", expression, quote(name));
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
                && Objects.equals(expression, that.expression)
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, notNull, relationship, expression);
    }

    @Override
    public String toString()
    {
        return "Column{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", notNull=" + notNull +
                ", relationship='" + relationship + '\'' +
                ", expression='" + expression + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    private static String quote(String name)
    {
        return String.format("\"%s\"", name);
    }
}
