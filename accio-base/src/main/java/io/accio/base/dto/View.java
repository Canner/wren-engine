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

import static java.util.Objects.requireNonNull;

public class View
{
    private final String name;
    private final String statement;
    private final String description;

    public static View view(String name, String statement)
    {
        return view(name, statement, null);
    }

    public static View view(String name, String statement, String description)
    {
        return new View(name, statement, description);
    }

    @JsonCreator
    public View(
            @JsonProperty("name") String name,
            @JsonProperty("statement") String statement,
            @JsonProperty("description") String description)
    {
        this.name = requireNonNull(name, "name is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.description = description;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getStatement()
    {
        return statement;
    }

    @JsonProperty
    public String getDescription()
    {
        return description;
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
        View view = (View) o;
        return Objects.equals(name, view.name) &&
                Objects.equals(statement, view.statement) &&
                Objects.equals(description, view.description);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                name,
                statement,
                description);
    }

    @Override
    public String toString()
    {
        return "View{" +
                "name='" + name + '\'' +
                ", statement='" + statement + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
