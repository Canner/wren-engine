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

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.Expression;
import io.wren.base.dto.JoinType;

import java.util.List;
import java.util.Objects;

public class PlannedRelationship
{
    private final RelationableReference from;
    private final RelationableReference to;
    private final Expression qualifiedCondition;
    private final JoinType joinType;

    public PlannedRelationship(
            RelationableReference from,
            RelationableReference to,
            Expression qualifiedCondition,
            JoinType joinType)
    {
        this.from = from;
        this.to = to;
        this.qualifiedCondition = qualifiedCondition;
        this.joinType = joinType;
    }

    public RelationableReference getFrom()
    {
        return from;
    }

    public RelationableReference getTo()
    {
        return to;
    }

    public Expression getQualifiedCondition()
    {
        return qualifiedCondition;
    }

    public JoinType getJoinType()
    {
        return joinType;
    }

    public List<RelationableReference> getRequriedRelationables()
    {
        return ImmutableList.of(from, to);
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
        PlannedRelationship that = (PlannedRelationship) o;
        return Objects.equals(from, that.from) &&
                Objects.equals(to, that.to) &&
                Objects.equals(qualifiedCondition, that.qualifiedCondition) &&
                joinType == that.joinType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(from, to, qualifiedCondition, joinType);
    }
}
