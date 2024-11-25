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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.Identifier;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.wren.base.Utils.checkArgument;
import static io.wren.base.Utils.requireNonNullEmpty;
import static io.wren.base.sqlrewrite.Utils.parseExpression;
import static java.util.Objects.requireNonNull;

public class Relationship
{
    private final String name;
    private final List<String> models;
    private final JoinType joinType;
    private final String condition;
    private final Expression qualifiedCondition;

    // for debugging internally
    private final boolean isReverse;
    private final List<SortKey> manySideSortKeys;

    public static Relationship relationship(String name, List<String> models, JoinType joinType, String condition)
    {
        return relationship(name, models, joinType, condition, null);
    }

    public static Relationship relationship(String name, List<String> models, JoinType joinType, String condition, List<SortKey> manySideSortKeys)
    {
        return new Relationship(name, models, joinType, condition, manySideSortKeys);
    }

    public static Relationship reverse(Relationship relationship)
    {
        return new Relationship(
                relationship.name,
                Lists.reverse(relationship.getModels()),
                JoinType.reverse(relationship.joinType),
                relationship.getCondition(),
                true,
                relationship.getManySideSortKeys());
    }

    @JsonCreator
    public Relationship(
            @JsonProperty("name") String name,
            @JsonProperty("models") List<String> models,
            @JsonProperty("joinType") JoinType joinType,
            @JsonProperty("condition") String condition,
            @JsonProperty("manySideSortKeys") List<SortKey> manySideSortKeys)
    {
        this(name, models, joinType, condition, false, manySideSortKeys);
    }

    public Relationship(
            String name,
            List<String> models,
            JoinType joinType,
            String condition,
            boolean isReverse,
            List<SortKey> manySideSortKeys)
    {
        this.name = requireNonNullEmpty(name, "name is null or empty");
        checkArgument(models != null && models.size() >= 2, "relationship should contain at least 2 models");
        this.models = models;
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.condition = requireNonNullEmpty(condition, "condition is null or empty");
        this.qualifiedCondition = qualifiedCondition(condition);
        this.isReverse = isReverse;
        this.manySideSortKeys = manySideSortKeys == null ? List.of() : manySideSortKeys;
    }

    private Expression qualifiedCondition(String condition)
    {
        Expression parsed = parseExpression(condition);
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new DereferenceExpression(
                        node.getLocation(),
                        treeRewriter.rewrite(node.getBase(), context),
                        node.getField().map(field -> treeRewriter.rewrite(field, context)));
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (node.isDelimited()) {
                    return node;
                }
                if (node.getLocation().isPresent()) {
                    return new Identifier(node.getLocation().get(), node.getValue(), true);
                }
                return new Identifier(node.getValue(), true);
            }
        }, parsed);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<String> getModels()
    {
        return models;
    }

    @JsonProperty
    public JoinType getJoinType()
    {
        return joinType;
    }

    @JsonProperty
    public String getCondition()
    {
        return condition;
    }

    public Expression getQualifiedCondition()
    {
        return qualifiedCondition;
    }

    @JsonProperty
    public List<SortKey> getManySideSortKeys()
    {
        return manySideSortKeys;
    }

    public boolean isReverse()
    {
        return isReverse;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Relationship that = (Relationship) obj;
        return Objects.equals(name, that.name) &&
                Objects.equals(models, that.models) &&
                joinType == that.joinType &&
                Objects.equals(condition, that.condition) &&
                isReverse == that.isReverse &&
                Objects.equals(manySideSortKeys, that.manySideSortKeys);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, models, joinType, condition, isReverse, manySideSortKeys);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("models", models)
                .add("joinType", joinType)
                .add("condition", condition)
                .add("isReverse", isReverse)
                .add("manySideSortKeys", manySideSortKeys)
                .toString();
    }

    public static class SortKey
    {
        public enum Ordering
        {
            ASC,
            DESC;

            public static Ordering get(String value)
            {
                return Arrays.stream(values())
                        .filter(v -> v.toString().equalsIgnoreCase(value))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("Unsupported ordering"));
            }
        }

        private final String name;
        private final boolean isDescending;

        @JsonCreator
        public SortKey(
                @JsonProperty("name") String name,
                @JsonProperty("ordering") Ordering ordering)
        {
            this.name = requireNonNull(name, "name is null");
            this.isDescending = ordering == Ordering.DESC;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public boolean isDescending()
        {
            return isDescending;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SortKey sortKey = (SortKey) obj;
            return Objects.equals(name, sortKey.name)
                    && isDescending == sortKey.isDescending;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, isDescending);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("isDescending", isDescending)
                    .toString();
        }
    }
}
