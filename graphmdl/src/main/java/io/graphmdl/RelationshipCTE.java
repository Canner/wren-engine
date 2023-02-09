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

package io.graphmdl;

import io.graphmdl.base.dto.Relationship;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class RelationshipCTE
{
    private final String name;
    private final Relation left;
    private final Relation right;

    private final Relationship relationship;

    public RelationshipCTE(String name, Relation left, Relation right, Relationship relationship)
    {
        this.name = name;
        this.left = left;
        this.right = right;
        this.relationship = relationship;
    }

    public String getName()
    {
        return name;
    }

    public Relation getLeft()
    {
        return left;
    }

    public Relation getRight()
    {
        return right;
    }

    public Relationship getRelationship()
    {
        return relationship;
    }

    public List<String> getOutputColumn()
    {
        return Stream.concat(right.getColumns().stream(), List.of(left.getJoinKey()).stream()).collect(toList());
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
