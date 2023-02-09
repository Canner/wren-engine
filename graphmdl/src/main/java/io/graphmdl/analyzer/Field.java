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

package io.graphmdl.analyzer;

import io.trino.sql.tree.QualifiedName;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

// TODO: rename this to ModelField

/**
 * Inspired by io.trino.sql.analyzer.Field
 */
public class Field
{
    // TODO: go check if relationAlias should be optional
    // e.g. select table.col_1 from select * from table; => is this legal ? this is false
    private final Optional<QualifiedName> relationAlias;
    private final String modelName;
    private final String columnName;
    private final Optional<String> name;

    private Field(Optional<QualifiedName> relationAlias, String modelName, String columnName, Optional<String> name)
    {
        this.relationAlias = requireNonNull(relationAlias, "relationAlias is null");
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.name = requireNonNull(name, "name is null");
    }

    public Optional<QualifiedName> getRelationAlias()
    {
        return relationAlias;
    }

    public String getModelName()
    {
        return modelName;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public boolean matchesPrefix(Optional<QualifiedName> prefix)
    {
        return prefix.isEmpty() || relationAlias.isPresent() && relationAlias.get().hasSuffix(prefix.get());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<QualifiedName> relationAlias = Optional.empty();
        private String modelName;
        private String columnName;
        private Optional<String> name = Optional.empty();

        public Builder() {}

        public Builder relationAlias(Optional<QualifiedName> relationAlias)
        {
            this.relationAlias = relationAlias;
            return this;
        }

        public Builder modelName(String modelName)
        {
            this.modelName = modelName;
            return this;
        }

        public Builder columnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public Builder name(Optional<String> name)
        {
            this.name = name;
            return this;
        }

        public Field build()
        {
            return new Field(relationAlias, modelName, columnName, name);
        }
    }
}
