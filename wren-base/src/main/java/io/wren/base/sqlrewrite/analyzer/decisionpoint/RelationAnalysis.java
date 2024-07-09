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

import io.trino.sql.tree.NodeLocation;

import java.util.List;

import static io.wren.base.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class RelationAnalysis
{
    static JoinRelation join(Type type, String alias, RelationAnalysis left, RelationAnalysis right, String criteria, List<ExprSource> exprSources, NodeLocation nodeLocation)
    {
        checkArgument(type != Type.TABLE && type != Type.SUBQUERY, "type should be a join type");
        return new JoinRelation(type, alias, left, right, criteria, exprSources, nodeLocation);
    }

    static TableRelation table(String tableName, String alias, NodeLocation nodeLocation)
    {
        return new TableRelation(tableName, alias, nodeLocation);
    }

    static SubqueryRelation subquery(String alias, List<QueryAnalysis> body, NodeLocation nodeLocation)
    {
        return new SubqueryRelation(alias, body, nodeLocation);
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
    private final NodeLocation nodeLocation;

    public RelationAnalysis(Type type, String alias, NodeLocation nodeLocation)
    {
        this.type = requireNonNull(type, "type is null");
        this.alias = alias;
        this.nodeLocation = nodeLocation;
    }

    public Type getType()
    {
        return type;
    }

    public String getAlias()
    {
        return alias;
    }

    public NodeLocation getNodeLocation()
    {
        return nodeLocation;
    }

    public static class JoinRelation
            extends RelationAnalysis
    {
        private final RelationAnalysis left;
        private final RelationAnalysis right;
        private final String criteria;
        private final List<ExprSource> exprSources;

        public JoinRelation(Type type, String alias, RelationAnalysis left, RelationAnalysis right, String criteria, List<ExprSource> exprSources, NodeLocation nodeLocation)
        {
            super(type, alias, nodeLocation);
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
            this.criteria = criteria;
            this.exprSources = exprSources == null ? List.of() : exprSources;
        }

        public RelationAnalysis getLeft()
        {
            return left;
        }

        public RelationAnalysis getRight()
        {
            return right;
        }

        public String getCriteria()
        {
            return criteria;
        }

        public List<ExprSource> getExprSources()
        {
            return exprSources;
        }
    }

    public static class TableRelation
            extends RelationAnalysis
    {
        private final String tableName;

        public TableRelation(String tableName, String alias, NodeLocation nodeLocation)
        {
            super(Type.TABLE, alias, nodeLocation);
            this.tableName = requireNonNull(tableName, "tableName is null");
        }

        public String getTableName()
        {
            return tableName;
        }
    }

    public static class SubqueryRelation
            extends RelationAnalysis
    {
        private final List<QueryAnalysis> body;

        public SubqueryRelation(String alias, List<QueryAnalysis> body, NodeLocation nodeLocation)
        {
            super(Type.SUBQUERY, alias, nodeLocation);
            this.body = requireNonNull(body, "body is null");
        }

        public List<QueryAnalysis> getBody()
        {
            return body;
        }
    }
}
