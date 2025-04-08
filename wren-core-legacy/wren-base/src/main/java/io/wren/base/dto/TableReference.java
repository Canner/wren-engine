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
import io.wren.base.Utils;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;

public class TableReference
{
    public static TableReference tableReference(String catalog, String schema, String table)
    {
        return new TableReference(catalog, schema, table);
    }

    private final String catalog;
    private final String schema;
    private final String table;

    @JsonCreator
    public TableReference(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.table = Utils.requireNonNullEmpty(table, "table should not be empty");
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    public String toQualifiedName()
    {
        if (isNullOrEmpty(catalog)) {
            if (isNullOrEmpty(schema)) {
                return format("\"%s\"", table);
            }
            return format("\"%s\".\"%s\"", schema, table);
        }
        return format("\"%s\".\"%s\".\"%s\"", catalog, schema, table);
    }

    private boolean isNullOrEmpty(String str)
    {
        return str == null || str.isEmpty();
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
        TableReference that = (TableReference) o;
        return Objects.equals(catalog, that.catalog) && Objects.equals(schema, that.schema) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schema, table);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalog", catalog)
                .add("schema", schema)
                .add("table", table)
                .toString();
    }
}
