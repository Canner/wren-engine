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

package io.wren.base.sqlrewrite;

import io.trino.sql.tree.Query;
import io.wren.base.WrenMDL;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationable;

import java.util.Objects;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RelationInfo
        implements QueryDescriptor
{
    private final Relationable relationable;
    private final Set<String> requiredObjects;
    private final Query query;

    public static RelationInfo get(Relationable relationable, WrenMDL mdl, Set<String> requiredFields)
    {
        if (relationable instanceof Model) {
            return new ModelSqlRender(relationable, mdl, requiredFields).render();
        }
        else if (relationable instanceof Metric) {
            return new MetricSqlRender((Metric) relationable, mdl, requiredFields).render();
        }
        else {
            throw new IllegalArgumentException(format("cannot get relation info from relationable %s", relationable));
        }
    }

    public static RelationInfo get(Relationable relationable, WrenMDL mdl)
    {
        if (relationable instanceof Model) {
            return new ModelSqlRender(relationable, mdl).render();
        }
        else if (relationable instanceof Metric) {
            return new MetricSqlRender((Metric) relationable, mdl).render();
        }
        else {
            throw new IllegalArgumentException(format("cannot get relation info from relationable %s", relationable));
        }
    }

    RelationInfo(
            Relationable relationable,
            Set<String> requiredModels,
            Query query)
    {
        this.relationable = requireNonNull(relationable);
        this.requiredObjects = requireNonNull(requiredModels);
        this.query = requireNonNull(query);
    }

    public Set<String> getRequiredObjects()
    {
        return requiredObjects;
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(relationable, requiredObjects, query);
    }

    @Override
    public String getName()
    {
        return relationable.getName();
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
        RelationInfo relationInfo = (RelationInfo) o;
        return Objects.equals(relationable, relationInfo.relationable)
                && Objects.equals(requiredObjects, relationInfo.requiredObjects)
                && Objects.equals(query, relationInfo.query);
    }
}
