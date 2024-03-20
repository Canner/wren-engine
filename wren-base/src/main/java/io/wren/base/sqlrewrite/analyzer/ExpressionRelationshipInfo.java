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

import io.trino.sql.tree.QualifiedName;
import io.wren.base.Utils;
import io.wren.base.dto.Relationship;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ExpressionRelationshipInfo
{
    private final QualifiedName qualifiedName;
    // for debug usage
    private final List<String> relationshipParts;
    private final List<String> remainingParts;
    private final List<Relationship> relationships;
    private final Relationship baseModelRelationship;
    private final List<RelationshipColumnInfo> relationshipColumnInfos;

    public ExpressionRelationshipInfo(
            QualifiedName qualifiedName,
            List<String> relationshipParts,
            List<String> remainingParts,
            List<RelationshipColumnInfo> relationshipColumnInfos,
            Relationship baseModelRelationship)
    {
        this.qualifiedName = requireNonNull(qualifiedName);
        this.relationshipParts = requireNonNull(relationshipParts);
        this.remainingParts = requireNonNull(remainingParts);
        this.baseModelRelationship = requireNonNull(baseModelRelationship);
        this.relationshipColumnInfos = requireNonNull(relationshipColumnInfos);
        this.relationships = relationshipColumnInfos.stream().map(RelationshipColumnInfo::getNormalizedRelationship).collect(toImmutableList());
        Utils.checkArgument(relationshipParts.size() + remainingParts.size() == qualifiedName.getParts().size(), "mismatch part size");
    }

    public QualifiedName getQualifiedName()
    {
        return qualifiedName;
    }

    public List<String> getRemainingParts()
    {
        return remainingParts;
    }

    public List<Relationship> getRelationships()
    {
        return relationships;
    }

    public List<RelationshipColumnInfo> getRelationshipColumnInfos()
    {
        return relationshipColumnInfos;
    }

    public Relationship getBaseModelRelationship()
    {
        return baseModelRelationship;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("qualifiedName", qualifiedName)
                .add("relationshipParts", relationshipParts)
                .add("remainingParts", remainingParts)
                .add("relationships", relationships)
                .add("baseModelRelationship", baseModelRelationship)
                .toString();
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
        ExpressionRelationshipInfo that = (ExpressionRelationshipInfo) o;
        return Objects.equals(qualifiedName, that.qualifiedName) &&
                Objects.equals(relationshipParts, that.relationshipParts) &&
                Objects.equals(remainingParts, that.remainingParts) &&
                Objects.equals(relationships, that.relationships) &&
                Objects.equals(baseModelRelationship, that.baseModelRelationship);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                qualifiedName,
                relationshipParts,
                remainingParts,
                relationships,
                baseModelRelationship);
    }
}
