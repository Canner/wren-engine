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

package io.accio.main.pgcatalog.function;

import java.util.Objects;

/**
 * TODO: analyze the type of argument expression
 *  https://github.com/Canner/canner-metric-layer/issues/92
 * <p>
 * We only support function overloading with different number of argument now. Because
 * the work of analyze the type of argument is too huge to implement, FunctionKey only
 * recognizes each function by its name and number of argument.
 */
class FunctionKey
{
    public static FunctionKey functionKey(String name, int numArgument)
    {
        return new FunctionKey(name, numArgument);
    }

    private final String name;
    private final int numArgument;

    private FunctionKey(String name, int numArgument)
    {
        this.name = name;
        this.numArgument = numArgument;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, numArgument);
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
        FunctionKey that = (FunctionKey) o;
        return numArgument == that.numArgument && Objects.equals(name, that.name);
    }
}
