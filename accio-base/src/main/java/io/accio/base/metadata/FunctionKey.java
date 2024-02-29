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

package io.accio.base.metadata;

import io.accio.base.type.AnyType;
import io.accio.base.type.PGType;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class FunctionKey
{
    public static FunctionKey functionKey(String name, List<PGType<?>> argumentTypes)
    {
        return new FunctionKey(name, argumentTypes);
    }

    private final String name;
    private final List<PGType<?>> arguments;

    private FunctionKey(String name, List<PGType<?>> arguments)
    {
        this.name = name;
        this.arguments = arguments;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, arguments);
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
        return Objects.equals(name, that.name)
                && equals(arguments, that.arguments);
    }

    private boolean equals(List<PGType<?>> arguments1, List<PGType<?>> arguments2)
    {
        if (arguments1.size() != arguments2.size()) {
            return false;
        }
        return IntStream.range(0, arguments1.size())
                .allMatch(i -> arguments1.get(i).equals(arguments2.get(i))
                        || arguments1.get(i).equals(AnyType.ANY)
                        || arguments2.get(i).equals(AnyType.ANY));
    }
}
