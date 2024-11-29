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
import io.airlift.units.Duration;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.checkArgument;
import static io.wren.base.Utils.requireNonNullEmpty;

public class Model
        implements CacheInfo, Relationable
{
    private final String name;
    private final String refSql;
    private final String baseObject;
    private final TableReference tableReference;
    private final List<Column> columns;
    private final String primaryKey;
    private final boolean cached;
    private final Duration refreshTime;

    public static Model model(String name, String refSql, List<Column> columns)
    {
        return model(name, refSql, columns, null);
    }

    public static Model model(String name, String refSql, List<Column> columns, boolean cached)
    {
        return new Model(name, refSql, null, null, columns, null, cached, null);
    }

    public static Model model(String name, String refSql, List<Column> columns, String primaryKey)
    {
        return new Model(name, refSql, null, null, columns, primaryKey, false, null);
    }

    public static Model onBaseObject(String name, String baseObject, List<Column> columns, String primaryKey)
    {
        return new Model(name, null, baseObject, null, columns, primaryKey, false, null);
    }

    public static Model onTableReference(String name, TableReference tableReference, List<Column> columns, String primaryKey)
    {
        return new Model(name, null, null, tableReference, columns, primaryKey, false, null);
    }

    @JsonCreator
    public Model(
            @JsonProperty("name") String name,
            @JsonProperty("refSql") String refSql,
            @JsonProperty("baseObject") String baseObject,
            @JsonProperty("tableReference") TableReference tableReference,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("primaryKey") String primaryKey,
            @JsonProperty("cached") boolean cached,
            @JsonProperty("refreshTime") Duration refreshTime)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        checkArgument(Stream.of(refSql, baseObject, tableReference).filter(Model::isNonNullOrNonEmpty).count() == 1,
                "either none or more than one of (refSql, baseObject, tableReference) are set");
        this.refSql = refSql;
        this.baseObject = baseObject;
        this.tableReference = tableReference;
        this.columns = columns == null ? List.of() : columns;
        this.primaryKey = primaryKey;
        this.cached = cached;
        this.refreshTime = refreshTime == null ? defaultRefreshTime : refreshTime;
    }

    private static boolean isNonNullOrNonEmpty(Object value)
    {
        if (value == null) {
            return false;
        }
        if (value instanceof String) {
            return !((String) value).isEmpty();
        }
        return true;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getRefSql()
    {
        return refSql;
    }

    @JsonProperty
    @Override
    public String getBaseObject()
    {
        return baseObject;
    }

    @JsonProperty
    public TableReference getTableReference()
    {
        return tableReference;
    }

    @Override
    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public String getPrimaryKey()
    {
        return primaryKey;
    }

    @Override
    @JsonProperty
    public boolean isCached()
    {
        return cached;
    }

    @Override
    @JsonProperty
    public Duration getRefreshTime()
    {
        return refreshTime;
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
        return cached == that.cached &&
                Objects.equals(name, that.name) &&
                Objects.equals(refSql, that.refSql) &&
                Objects.equals(baseObject, that.baseObject) &&
                Objects.equals(tableReference, that.tableReference) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(primaryKey, that.primaryKey) &&
                Objects.equals(refreshTime, that.refreshTime);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, refSql, baseObject, tableReference, columns, primaryKey);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("refSql", refSql)
                .add("baseObject", baseObject)
                .add("tableReference", tableReference)
                .add("columns", columns)
                .add("cached", cached)
                .add("refreshTime", refreshTime)
                .toString();
    }
}
