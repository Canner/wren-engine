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

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class RelationType
{
    private final List<Field> fields;

    public RelationType()
    {
        this.fields = ImmutableList.of();
    }

    public RelationType(List<Field> fields)
    {
        this.fields = ImmutableList.copyOf(fields);
    }

    public List<Field> getFields()
    {
        return fields;
    }

    /**
     * get the columns matching the specified name
     */
    public List<Field> resolveFields(QualifiedName name)
    {
        return fields.stream()
                .filter(input -> input.getSourceColumn().stream().anyMatch(column -> column.getRelationship().isEmpty()))
                .filter(input -> input.canResolve(name))
                .collect(toImmutableList());
    }

    public Optional<Field> resolveAnyField(QualifiedName name)
    {
        return fields.stream()
                .filter(input -> input.getSourceColumn().stream().anyMatch(column -> column.getRelationship().isEmpty()))
                .filter(input -> input.canResolve(name))
                .findAny();
    }

    /**
     * Creates a new tuple descriptor containing all fields from this tuple descriptor
     * and all fields from the specified tuple descriptor.
     */
    public RelationType joinWith(RelationType other)
    {
        List<Field> fields = ImmutableList.<Field>builder()
                .addAll(this.fields)
                .addAll(other.fields)
                .build();

        return new RelationType(fields);
    }
}
