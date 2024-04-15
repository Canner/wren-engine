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

import io.wren.base.dto.Relationable;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RelationableReference
{
    private final Optional<Relationable> relationable;
    private final String name;
    private final boolean isOriginal;

    public RelationableReference(Optional<Relationable> relationable, String name, boolean isOriginal)
    {
        this.relationable = requireNonNull(relationable, "relationable is null");
        this.name = requireNonNull(name, "name is null");
        this.isOriginal = isOriginal;
    }

    public Optional<Relationable> getRelationable()
    {
        return relationable;
    }

    public String getName()
    {
        return name;
    }

    public boolean isOriginal()
    {
        return isOriginal;
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
        RelationableReference reference = (RelationableReference) o;
        return isOriginal == reference.isOriginal && Objects.equals(name, reference.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, isOriginal);
    }

    @Override
    public String toString()
    {
        return name;
    }
}
