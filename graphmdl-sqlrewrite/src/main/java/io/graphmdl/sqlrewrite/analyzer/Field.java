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

package io.graphmdl.sqlrewrite.analyzer;

import io.graphmdl.base.CatalogSchemaTableName;
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
    private final CatalogSchemaTableName modelName;
    private final String columnName;
    private final Optional<String> name;
    private final boolean isRelationship;

    private Field(
            Optional<QualifiedName> relationAlias,
            CatalogSchemaTableName modelName,
            String columnName,
            Optional<String> name,
            boolean isRelationship)
    {
        this.relationAlias = requireNonNull(relationAlias, "relationAlias is null");
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.name = requireNonNull(name, "name is null");
        this.isRelationship = isRelationship;
    }

    public Optional<QualifiedName> getRelationAlias()
    {
        return relationAlias;
    }

    public CatalogSchemaTableName getModelName()
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

    public boolean isRelationship()
    {
        return isRelationship;
    }

    public boolean matchesPrefix(Optional<QualifiedName> prefix)
    {
        return prefix.isEmpty() || relationAlias.isPresent() && relationAlias.get().hasSuffix(prefix.get());
    }

    /*
      Namespaces can have names such as "x", "x.y" or "" if there's no name
      Name to resolve can have names like "a", "x.a", "x.y.a"

      namespace  name     possible match
       ""         "a"           y
       "x"        "a"           y
       "x.y"      "a"           y

       ""         "x.a"         n
       "x"        "x.a"         y
       "x.y"      "x.a"         n

       ""         "x.y.a"       n
       "x"        "x.y.a"       n
       "x.y"      "x.y.a"       n

       ""         "y.a"         n
       "x"        "y.a"         n
       "x.y"      "y.a"         y
     */
    public boolean canResolve(QualifiedName name)
    {
        if (this.name.isEmpty()) {
            return false;
        }

        // TODO: need to know whether the qualified name and the name of this field were quoted
        return matchesPrefix(name.getPrefix()) && this.name.get().equalsIgnoreCase(name.getSuffix());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<QualifiedName> relationAlias = Optional.empty();
        private CatalogSchemaTableName modelName;
        private String columnName;
        private Optional<String> name = Optional.empty();
        private boolean isRelationship;

        public Builder() {}

        public Builder like(Field field)
        {
            this.relationAlias = field.relationAlias;
            this.modelName = field.modelName;
            this.columnName = field.columnName;
            this.name = field.name;
            this.isRelationship = field.isRelationship;
            return this;
        }

        public Builder relationAlias(Optional<QualifiedName> relationAlias)
        {
            this.relationAlias = relationAlias;
            return this;
        }

        public Builder modelName(CatalogSchemaTableName modelName)
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

        public Builder isRelationship(boolean isRelationship)
        {
            this.isRelationship = isRelationship;
            return this;
        }

        public Field build()
        {
            return new Field(relationAlias, modelName, columnName, name, isRelationship);
        }
    }
}
