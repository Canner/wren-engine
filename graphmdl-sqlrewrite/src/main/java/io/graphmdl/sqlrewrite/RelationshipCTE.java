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

package io.graphmdl.sqlrewrite;

import io.graphmdl.base.dto.Relationship;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * Defines how to compose a Relationship Common Table Expression (CTE). A relationship defines how to map a source table to a target table.
 * For example, if a relationship defines a connection between the User and Book tables,
 * the User table is considered the source table and the Book table is considered the target table.
 * <p>
 * For a TO_ONE relationship, all columns in the CTE are target objects.
 * <p>
 * For a TO_MANY relationship, only the relationship field is target object.
 **/
public class RelationshipCTE
{
    private final String name;
    private final Relation source;
    private final Relation target;

    private final String index;
    private final Relationship relationship;
    // The base key is the primary key of the base model.
    private final String baseKey;

    public RelationshipCTE(String name, Relation source, Relation target, Relationship relationship, String index, String baseKey)
    {
        this.name = name;
        this.source = source;
        this.target = target;
        this.relationship = relationship;
        this.index = index;
        this.baseKey = baseKey;
    }

    public String getName()
    {
        return name;
    }

    public Relation getSource()
    {
        return source;
    }

    public Relation getTarget()
    {
        return target;
    }

    public Relation getManySide()
    {
        switch (relationship.getJoinType()) {
            case MANY_TO_ONE:
                return source;
            case ONE_TO_MANY:
                return target;
        }
        throw new IllegalArgumentException(format("join type %s can't get many side", relationship.getJoinType()));
    }

    public Relationship getRelationship()
    {
        return relationship;
    }

    public List<String> getOutputColumn()
    {
        return Stream.concat(target.getColumns().stream(), List.of(source.getJoinKey()).stream()).collect(toList());
    }

    public Optional<String> getIndex()
    {
        return Optional.ofNullable(index);
    }

    public String getBaseKey()
    {
        return baseKey;
    }

    public static class Relation
    {
        private final String name;
        private final List<String> columns;
        private final String primaryKey;
        private final String joinKey;

        public Relation(String name, List<String> columns, String primaryKey, String joinKey)
        {
            this.name = name;
            this.columns = columns;
            this.primaryKey = primaryKey;
            this.joinKey = joinKey;
        }

        public String getName()
        {
            return name;
        }

        public List<String> getColumns()
        {
            return columns;
        }

        public String getPrimaryKey()
        {
            return primaryKey;
        }

        public String getJoinKey()
        {
            return joinKey;
        }
    }
}
