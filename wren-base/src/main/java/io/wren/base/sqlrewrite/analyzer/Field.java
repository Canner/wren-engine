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

package io.wren.base.sqlrewrite.analyzer;

import io.trino.sql.tree.QualifiedName;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.dto.Column;
import io.wren.base.sqlrewrite.Utils;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Inspired by io.trino.sql.analyzer.Field
 */
public class Field
{
    // TODO: go check if relationAlias should be optional
    // e.g. select table.col_1 from select * from table; => is this legal ? this is false
    private final Optional<QualifiedName> relationAlias;
    private final CatalogSchemaTableName tableName;
    // the name of the column in the table
    private final String columnName;
    // the name of the dataset where the column comes from
    private final Optional<String> sourceDatasetName;
    // the name of the column in the dataset where the column comes from
    private final Optional<Column> sourceColumn;
    // the name of the column in the query (If the column is aliased, this is the alias, otherwise it's the column name)
    private final Optional<String> name;

    private Field(
            QualifiedName relationAlias,
            CatalogSchemaTableName tableName,
            String columnName,
            String name,
            String sourceDatasetName,
            Column sourceColumn)
    {
        this.relationAlias = Optional.ofNullable(relationAlias);
        this.tableName = requireNonNull(tableName, "modelName is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.name = Optional.ofNullable(name);
        this.sourceDatasetName = Optional.ofNullable(sourceDatasetName);
        this.sourceColumn = Optional.ofNullable(sourceColumn);
    }

    public Optional<QualifiedName> getRelationAlias()
    {
        return relationAlias;
    }

    public CatalogSchemaTableName getTableName()
    {
        return tableName;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public Optional<String> getSourceDatasetName()
    {
        return sourceDatasetName;
    }

    public Optional<Column> getSourceColumn()
    {
        return sourceColumn;
    }

    public boolean matchesPrefix(Optional<QualifiedName> prefix)
    {
        return prefix.isEmpty() || relationAlias.orElse(Utils.toQualifiedName(tableName)).hasSuffix(prefix.get());
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
        if (name == null || this.name.isEmpty()) {
            return false;
        }

        // TODO: need to know whether the qualified name and the name of this field were quoted
        return (matchesPrefix(name.getPrefix()) && this.name.get().equalsIgnoreCase(name.getSuffix())) ||
        name.getPrefix().map(p -> p.toString().equals(columnName)).orElse(false); // support struct type
    }

    @Override
    public String toString()
    {
        return "Field{" +
                "relationAlias=" + relationAlias +
                ", tableName=" + tableName +
                ", columnName='" + columnName + '\'' +
                ", name=" + name +
                ", sourceDatasetName=" + sourceDatasetName +
                ", sourceColumn=" + sourceColumn +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private QualifiedName relationAlias;
        private CatalogSchemaTableName tableName;
        private String columnName;
        private String name;
        private String sourceModelName;
        private Column sourceColumn;

        public Builder() {}

        public Builder like(Field field)
        {
            this.relationAlias = field.relationAlias.orElse(null);
            this.tableName = field.tableName;
            this.columnName = field.columnName;
            this.name = field.name.orElse(null);
            this.sourceModelName = field.sourceDatasetName.orElse(null);
            this.sourceColumn = field.sourceColumn.orElse(null);
            return this;
        }

        public Builder relationAlias(QualifiedName relationAlias)
        {
            this.relationAlias = relationAlias;
            return this;
        }

        public Builder tableName(CatalogSchemaTableName tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder columnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder sourceModelName(String sourceModelName)
        {
            this.sourceModelName = sourceModelName;
            return this;
        }

        public Builder sourceColumn(Column sourceColumn)
        {
            this.sourceColumn = sourceColumn;
            return this;
        }

        public Field build()
        {
            return new Field(relationAlias, tableName, columnName, name, sourceModelName, sourceColumn);
        }
    }
}
