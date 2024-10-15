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

import static io.wren.base.dto.JoinType.GenericJoinType.TO_MANY;
import static io.wren.base.dto.JoinType.GenericJoinType.TO_ONE;
import static java.util.Objects.requireNonNull;

public enum JoinType
{
    MANY_TO_MANY(TO_MANY),
    ONE_TO_ONE(TO_ONE),
    MANY_TO_ONE(TO_ONE),
    ONE_TO_MANY(TO_MANY);

    public enum GenericJoinType
    {
        TO_ONE,
        TO_MANY,
    }

    private final GenericJoinType type;

    JoinType(GenericJoinType type)
    {
        this.type = requireNonNull(type);
    }

    public static JoinType reverse(JoinType joinType)
    {
        return switch (joinType) {
            case ONE_TO_ONE -> ONE_TO_ONE;
            case ONE_TO_MANY -> MANY_TO_ONE;
            case MANY_TO_ONE -> ONE_TO_MANY;
            case MANY_TO_MANY -> MANY_TO_MANY;
        };
    }

    public GenericJoinType getType()
    {
        return type;
    }

    public boolean isToOne()
    {
        return type == TO_ONE;
    }

    public boolean isToMany()
    {
        return type == TO_MANY;
    }
}
