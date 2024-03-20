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

package io.wren.base.metadata;

import io.wren.base.type.PGType;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Function
{
    protected final String name;
    protected final List<Argument> arguments;
    protected final PGType returnType;

    public Function(String name, List<Argument> arguments, PGType returnType)
    {
        this.name = name;
        this.arguments = arguments;
        this.returnType = returnType;
    }

    public String getName()
    {
        return name;
    }

    public Optional<List<Argument>> getArguments()
    {
        return Optional.ofNullable(arguments);
    }

    public Optional<PGType> getReturnType()
    {
        return Optional.ofNullable(returnType);
    }

    public static class Argument
    {
        public static Argument argument(String name, PGType type)
        {
            return new Argument(name, type);
        }

        private final String name;
        private final PGType type;

        public Argument(String name, PGType type)
        {
            this.name = name;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public PGType getType()
        {
            return type;
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String name;
        private List<Argument> arguments;
        private PGType returnType;

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder setArguments(List<Argument> arguments)
        {
            this.arguments = arguments;
            return this;
        }

        public Builder setReturnType(PGType returnType)
        {
            this.returnType = returnType;
            return this;
        }

        public Function build()
        {
            requireNonNull(name, "name is null");
            return new Function(name, arguments, returnType);
        }
    }
}
