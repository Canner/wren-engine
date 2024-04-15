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
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RelationInfo
        implements QueryDescriptor
{
    private final RelationableReference reference;
    private final Set<String> requiredObjects;
    private final Query query;

    public static RelationInfo get(RelationableReference reference, WrenMDL mdl, Set<String> requiredFields)
    {
        return reference.getRelationable().map(relationable -> switch (relationable) {
            case Model model -> model.getBaseObject() == null ?
                    new ModelRelationSqlRender(reference, mdl, requiredFields).render() :
                    new DerivedModelSqlRender(reference, mdl, requiredFields).render();
            case Metric _ -> new MetricSqlRender(reference, mdl, requiredFields).render();
            default -> null;
        }).orElseThrow(() -> new IllegalArgumentException(format("cannot get relation info from relationable %s", reference)));
    }

    public static RelationInfo get(RelationableReference reference, WrenMDL mdl)
    {
        return reference.getRelationable().map(relationable -> switch (relationable) {
            case Model model -> model.getBaseObject() == null ?
                    new ModelRelationSqlRender(reference, mdl).render() :
                    new DerivedModelSqlRender(reference, mdl).render();
            case Metric _ -> new MetricSqlRender(reference, mdl).render();
            default -> null;
        }).orElseThrow(() -> new IllegalArgumentException(format("cannot get relation info from relationable %s", reference)));
    }

    RelationInfo(
            RelationableReference reference,
            Set<String> requiredModels,
            Query query)
    {
        this.reference = requireNonNull(reference);
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
    public Optional<Relationable> getRelationable()
    {
        return reference.getRelationable();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(reference, requiredObjects, query);
    }

    @Override
    public String getName()
    {
        return reference.getName();
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
        return Objects.equals(reference, relationInfo.reference)
                && Objects.equals(requiredObjects, relationInfo.requiredObjects)
                && Objects.equals(query, relationInfo.query);
    }
}
