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

package io.graphmdl.base.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Model
{
    private final String name;
    private final String refSql;
    private final List<Column> columns;
    private final String primaryKey;

    public static Model model(String name, String refSql, List<Column> columns)
    {
        return new Model(name, refSql, columns, null);
    }

    public static Model model(String name, String refSql, List<Column> columns, String primaryKey)
    {
        return new Model(name, refSql, columns, primaryKey);
    }

    @JsonCreator
    public Model(
            @JsonProperty("name") String name,
            @JsonProperty("refSql") String refSql,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("primaryKey") String primaryKey)
    {
        this.name = requireNonNull(name, "name is null");
        this.refSql = requireNonNull(refSql, "refSql is null");
        this.columns = columns == null ? List.of() : columns;
        this.primaryKey = primaryKey;
    }

    public String getName()
    {
        return name;
    }

    public String getRefSql()
    {
        return refSql;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public String getPrimaryKey()
    {
        return primaryKey;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Model that = (Model) obj;
        return Objects.equals(name, that.name)
                && Objects.equals(refSql, that.refSql)
                && Objects.equals(columns, that.columns)
                && Objects.equals(primaryKey, that.primaryKey);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, refSql, columns, primaryKey);
    }
}
