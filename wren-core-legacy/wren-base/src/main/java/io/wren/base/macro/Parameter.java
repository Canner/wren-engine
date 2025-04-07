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

package io.wren.base.macro;

import java.util.Objects;

public class Parameter
{
    public static Parameter expressionType(String name)
    {
        return new Parameter(name, TYPE.EXPRESSION);
    }

    public static Parameter macroType(String name)
    {
        return new Parameter(name, TYPE.MACRO);
    }

    public enum TYPE
    {
        MACRO,
        EXPRESSION
    }

    private final String name;
    private final TYPE type;

    public Parameter(String name, TYPE type)
    {
        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public TYPE getType()
    {
        return type;
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
        Parameter parameter = (Parameter) o;
        return Objects.equals(name, parameter.name) && type == parameter.type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public String toString()
    {
        return "Parameter{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }
}
