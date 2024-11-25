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

package io.wren.base.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.wren.base.macro.Parameter;
import io.wren.base.macro.ParameterListParser;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.checkArgument;
import static io.wren.base.Utils.requireNonNullEmpty;
import static java.lang.String.format;

public class Macro
{
    public static Macro macro(String name, String definition)
    {
        return new Macro(name, definition);
    }

    private final String name;
    private final String definition;
    private final List<Parameter> parameters;
    private final String body;

    @JsonCreator
    public Macro(
            @JsonProperty("name") String name,
            @JsonProperty("definition") String definition)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        this.definition = requireNonNullEmpty(definition, "definition is null or empty");
        String[] split = definition.split("=>", 2);
        checkArgument(split.length == 2, format("definition is invalid: %s", definition));
        String paramString = split[0].trim();
        String body = split[1].trim();
        this.parameters = new ParameterListParser().parse(paramString);
        this.body = body;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getDefinition()
    {
        return definition;
    }

    public List<Parameter> getParameters()
    {
        return parameters;
    }

    public String getBody()
    {
        return body;
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
        Macro macro = (Macro) o;
        return Objects.equals(name, macro.name) &&
                Objects.equals(parameters, macro.parameters) &&
                Objects.equals(body, macro.body);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, parameters, body);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("parameters", parameters)
                .add("body", body)
                .toString();
    }
}
