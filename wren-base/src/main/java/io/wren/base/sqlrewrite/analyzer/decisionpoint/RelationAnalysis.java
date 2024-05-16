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

package io.wren.base.sqlrewrite.analyzer.decisionpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

import static io.wren.base.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = RelationAnalysis.TableRelation.class, name = "TABLE"),
        @JsonSubTypes.Type(value = RelationAnalysis.SubqueryRelation.class, name = "SUBQUERY"),
        @JsonSubTypes.Type(value = RelationAnalysis.JoinRelation.class, name = "INNER_JOIN"),
        @JsonSubTypes.Type(value = RelationAnalysis.JoinRelation.class, name = "LEFT_JOIN"),
        @JsonSubTypes.Type(value = RelationAnalysis.JoinRelation.class, name = "RIGHT_JOIN"),
        @JsonSubTypes.Type(value = RelationAnalysis.JoinRelation.class, name = "FULL_JOIN"),
        @JsonSubTypes.Type(value = RelationAnalysis.JoinRelation.class, name = "CROSS_JOIN"),
        @JsonSubTypes.Type(value = RelationAnalysis.JoinRelation.class, name = "IMPLICIT_JOIN"),
})

public abstract class RelationAnalysis
{
    static JoinRelation join(Type type, String alias, RelationAnalysis left, RelationAnalysis right, String criteria)
    {
        checkArgument(type != Type.TABLE && type != Type.SUBQUERY, "type should be a join type");
        return new JoinRelation(type, alias, left, right, criteria);
    }

    static TableRelation table(String tableName, String alias)
    {
        return new TableRelation(tableName, alias);
    }

    static SubqueryRelation subquery(String alias, List<QueryAnalysis> body)
    {
        return new SubqueryRelation(alias, body);
    }

    public enum Type
    {
        TABLE,
        SUBQUERY,
        INNER_JOIN,
        LEFT_JOIN,
        RIGHT_JOIN,
        FULL_JOIN,
        CROSS_JOIN,
        IMPLICIT_JOIN,
    }

    private final Type type;
    private final String alias;

    public RelationAnalysis(Type type, String alias)
    {
        this.type = requireNonNull(type, "type is null");
        this.alias = alias;
    }

    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getAlias()
    {
        return alias;
    }

    public static class JoinRelation
            extends RelationAnalysis
    {
        private final RelationAnalysis left;
        private final RelationAnalysis right;
        private final String criteria;

        @JsonCreator
        public JoinRelation(Type type, String alias, RelationAnalysis left, RelationAnalysis right, String criteria)
        {
            super(type, alias);
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
            this.criteria = criteria;
        }

        @JsonProperty
        public RelationAnalysis getLeft()
        {
            return left;
        }

        @JsonProperty
        public RelationAnalysis getRight()
        {
            return right;
        }

        @JsonProperty
        public String getCriteria()
        {
            return criteria;
        }
    }

    public static class TableRelation
            extends RelationAnalysis
    {
        private final String tableName;

        @JsonCreator
        public TableRelation(String tableName, String alias)
        {
            super(Type.TABLE, alias);
            this.tableName = requireNonNull(tableName, "tableName is null");
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }
    }

    public static class SubqueryRelation
            extends RelationAnalysis
    {
        private final List<QueryAnalysis> body;

        @JsonCreator
        public SubqueryRelation(String alias, List<QueryAnalysis> body)
        {
            super(Type.SUBQUERY, alias);
            this.body = requireNonNull(body, "body is null");
        }

        @JsonProperty
        public List<QueryAnalysis> getBody()
        {
            return body;
        }
    }
}
