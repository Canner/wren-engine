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

import com.google.common.base.Joiner;
import io.accio.base.type.PGType;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PgFunction
{
    public static final Pattern PG_FUNCTION_PATTERN = Pattern.compile("(?<functionName>[a-zA-Z]+(_[a-zA-Z0-9]+)*)(__(?<argsType>[a-zA-Z]+(_[a-zA-Z0-9]+)*))?(___(?<returnType>[a-zA-Z]+(_[a-zA-Z0-9]+)*))?");

    public enum Language
    {
        SQL,
        JS
    }

    public static PgFunction.Builder builder()
    {
        return new Builder();
    }

    private final String name;
    private final Language language;

    private final List<Argument> arguments;

    private final PGType returnType;

    private final String definition;
    private final boolean subquery;
    // if the function is implemented in the database
    private final boolean implemented;

    public PgFunction(
            String name,
            Language language,
            List<Argument> arguments,
            PGType returnType,
            String definition,
            boolean subquery,
            boolean implemented)
    {
        this.name = name;
        this.language = language;
        this.arguments = arguments;
        this.returnType = returnType;
        this.definition = definition;
        this.subquery = subquery;
        this.implemented = implemented;
    }

    public String getName()
    {
        return name;
    }

    /**
     * Some data warehouse(BigQuery) doesn't support function overloading. We should name the function with its argument's type and return type.
     * For example:
     * pg_relation_size(relOid int)bigint -> pg_relation_size__int___bigint(relOid int)
     * pg_relation_size(relOid int, text varchar)bigint -> pg_relation_size__int_varchar___bigint(relOid int, text varchar)
     *
     * @return the name used by the remote database.
     */
    public String getRemoteName()
    {
        String argString = getArguments().isPresent() ? "__" + Joiner.on("_").join(arguments.stream().map(Argument::getType).map(PGType::typName).collect(toImmutableList())) : "";
        String returnString = getReturnType().isPresent() ? "___" + returnType.typName() : "";
        return getName() + argString + returnString;
    }

    public Language getLanguage()
    {
        return language;
    }

    public Optional<List<Argument>> getArguments()
    {
        return Optional.ofNullable(arguments);
    }

    public Optional<PGType> getReturnType()
    {
        return Optional.ofNullable(returnType);
    }

    public String getDefinition()
    {
        return definition;
    }

    public boolean isSubquery()
    {
        return subquery;
    }

    public boolean isImplemented()
    {
        return implemented;
    }

    @Override
    public String toString()
    {
        StringBuilder parameterBuilder = new StringBuilder();
        if (getArguments().isPresent()) {
            for (Argument argument : getArguments().get()) {
                parameterBuilder
                        .append(argument.getName()).append(" ")
                        .append(argument.getType()).append(",");
            }
            parameterBuilder.setLength(parameterBuilder.length() - 1);
        }

        return format("%s(%s)%s", getName(), parameterBuilder, getReturnType().isPresent() ? returnType.typName() : "void");
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

    public static class Builder
    {
        private String name;
        private Language language;

        private String definition;

        private List<Argument> arguments;

        private PGType returnType;
        private boolean subquery;
        private boolean implemented;

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder setLanguage(Language language)
        {
            this.language = language;
            return this;
        }

        public Builder setDefinition(String definition)
        {
            this.definition = definition;
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

        public Builder setSubquery(boolean subquery)
        {
            this.subquery = subquery;
            return this;
        }

        public Builder setImplemented(boolean implemented)
        {
            this.implemented = implemented;
            return this;
        }

        public PgFunction build()
        {
            requireNonNull(name, "name is null");
            requireNonNull(language, "language is null");
            return new PgFunction(name, language, arguments, returnType, definition, subquery, implemented);
        }
    }
}
