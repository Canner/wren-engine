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

package io.wren.base;

import java.util.Objects;

public class Parameter
{
    private final String type;
    private final Object value;

    public Parameter(String type, Object value)
    {
        this.type = type;
        this.value = value;
    }

    public String getType()
    {
        return type;
    }

    public Object getValue()
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
        Parameter parameter = (Parameter) o;
        return Objects.equals(type, parameter.type) &&
                Objects.equals(value, parameter.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, value);
    }
}
