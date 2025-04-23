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

import io.wren.base.dto.Column;
import io.wren.base.dto.Model;
import io.wren.base.dto.Relationship;

import static java.util.Objects.requireNonNull;

public class RelationshipColumnInfo
{
    private final Column column;
    private final Relationship normalizedRelationship;
    private final Model model;

    /**
     * relationship column info
     *
     * @param model the model that relationship column belongs to
     * @param column the column that indicate to the relationship column
     * @param relationship the relationship defined in the column
     */
    public RelationshipColumnInfo(Model model, Column column, Relationship relationship)
    {
        this.model = requireNonNull(model);
        this.column = requireNonNull(column);
        // reverse the models order in relationship if needed
        this.normalizedRelationship = reverseIfNeeded(relationship, column.getType());
    }

    public Column getColumn()
    {
        return column;
    }

    public Model getModel()
    {
        return model;
    }

    public Relationship getNormalizedRelationship()
    {
        return normalizedRelationship;
    }

    private static Relationship reverseIfNeeded(Relationship relationship, String firstModelName)
    {
        if (relationship.getModels().get(1).equals(firstModelName)) {
            return relationship;
        }
        return Relationship.reverse(relationship);
    }
}
