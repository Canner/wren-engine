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

package io.cml.graphml.base.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Column
{
    private final String name;
    private final String type;

    private final boolean notNull;
    private final Optional<String> relationship;
    private final Optional<String> expression;
    private final boolean isTime;

    public static Column column(String name, String type, String relationship, boolean notNull)
    {
        return new Column(name, type, relationship, notNull, null, false);
    }

    public static Column measure(String name, String type, String relationship, boolean notNull, String expression)
    {
        return new Column(name, type, relationship, notNull, expression, false);
    }

    public static Column time(String name, String type, boolean notNull)
    {
        return new Column(name, type, null, notNull, null, true);
    }
    

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("relationship") String relationship,
            @JsonProperty("notNull") boolean notNull,
            @JsonProperty("expression") String expression,
            @JsonProperty("isTime") boolean isTime)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.relationship = Optional.ofNullable(relationship);
        this.notNull = notNull;
        this.expression = Optional.ofNullable(expression);
        this.isTime = isTime;

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
        return relationship;
    }

    public boolean isNotNull()
    {
        return notNull;
    }

    public Optional<String> getExpression()
    {
        return expression;
    }

    public String getSqlExpression()
    {
        if (expression.isEmpty()) {
            return getName();
        }

        return format("%s as %s", expression.get(), name);
    }
}
