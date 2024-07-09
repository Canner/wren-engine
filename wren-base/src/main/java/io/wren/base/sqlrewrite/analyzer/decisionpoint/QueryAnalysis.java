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
import io.trino.sql.tree.SortItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class QueryAnalysis
{
    public static Builder builder()
    {
        return new Builder();
    }

    private final List<ColumnAnalysis> selectItems;
    private final RelationAnalysis relation;
    private final FilterAnalysis filter;
    private final List<List<GroupByKey>> groupByKeys;
    private final List<SortItemAnalysis> sortings;
    private final boolean isSubqueryOrCte;

    public QueryAnalysis(
            List<ColumnAnalysis> selectItems,
            RelationAnalysis relation,
            FilterAnalysis filter,
            List<List<GroupByKey>> groupByKeys,
            List<SortItemAnalysis> sortings,
            boolean isSubqueryOrCte)
    {
        this.selectItems = selectItems == null ? List.of() : List.copyOf(selectItems);
        this.relation = relation;
        this.filter = filter;
        this.groupByKeys = groupByKeys == null ? List.of() : List.copyOf(groupByKeys);
        this.sortings = sortings == null ? List.of() : List.copyOf(sortings);
        this.isSubqueryOrCte = isSubqueryOrCte;
    }

    public List<ColumnAnalysis> getSelectItems()
    {
        return selectItems;
    }

    public RelationAnalysis getRelation()
    {
        return relation;
    }

    public FilterAnalysis getFilter()
    {
        return filter;
    }

    public List<List<GroupByKey>> getGroupByKeys()
    {
        return groupByKeys;
    }

    public List<SortItemAnalysis> getSortings()
    {
        return sortings;
    }

    public boolean isSubqueryOrCte()
    {
        return isSubqueryOrCte;
    }

    public static class ColumnAnalysis
    {
        private final Optional<String> aliasName;
        private final String expression;
        private final Map<String, String> properties;
        private final NodeLocation nodeLocation;
        private final List<ExprSource> exprSources;

        public ColumnAnalysis(Optional<String> aliasName, String expression, Map<String, String> properties, NodeLocation nodeLocation, List<ExprSource> exprSources)
        {
            this.aliasName = aliasName;
            this.expression = expression;
            this.properties = properties;
            this.nodeLocation = nodeLocation;
            this.exprSources = exprSources == null ? List.of() : List.copyOf(exprSources);
        }

        public Optional<String> getAliasName()
        {
            return aliasName;
        }

        public String getExpression()
        {
            return expression;
        }

        public Map<String, String> getProperties()
        {
            return properties;
        }

        public NodeLocation getNodeLocation()
        {
            return nodeLocation;
        }

        public List<ExprSource> getExprSources()
        {
            return exprSources;
        }
    }

    public static class SortItemAnalysis
    {
        private final String expression;
        private final SortItem.Ordering ordering;
        private final NodeLocation nodeLocation;
        private final List<ExprSource> exprSources;

        public SortItemAnalysis(String expression, SortItem.Ordering ordering, NodeLocation nodeLocation, List<ExprSource> exprSources)
        {
            this.expression = expression;
            this.ordering = ordering;
            this.nodeLocation = nodeLocation;
            this.exprSources = exprSources == null ? List.of() : List.copyOf(exprSources);
        }

        public String getExpression()
        {
            return expression;
        }

        public SortItem.Ordering getOrdering()
        {
            return ordering;
        }

        public NodeLocation getNodeLocation()
        {
            return nodeLocation;
        }

        public List<ExprSource> getExprSources()
        {
            return exprSources;
        }
    }

    public static class GroupByKey
    {
        private final String expression;
        private final NodeLocation nodeLocation;
        private final List<ExprSource> exprSources;

        public GroupByKey(String expression, NodeLocation nodeLocation, List<ExprSource> exprSources)
        {
            this.expression = expression;
            this.nodeLocation = nodeLocation;
            this.exprSources = exprSources == null ? List.of() : List.copyOf(exprSources);
        }

        public String getExpression()
        {
            return expression;
        }

        public NodeLocation getNodeLocation()
        {
            return nodeLocation;
        }

        public List<ExprSource> getExprSources()
        {
            return exprSources;
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
            GroupByKey that = (GroupByKey) o;
            return Objects.equals(expression, that.expression) &&
                    Objects.equals(nodeLocation, that.nodeLocation) &&
                    Objects.equals(exprSources, that.exprSources);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expression, nodeLocation, exprSources);
        }

        @Override
        public String toString()
        {
            return "GroupByKey{" +
                    "expression='" + expression + '\'' +
                    ", nodeLocation=" + nodeLocation +
                    ", exprSources=" + exprSources +
                    '}';
        }
    }

    public static class Builder
    {
        private final List<ColumnAnalysis> selectItems = new ArrayList<>();
        private RelationAnalysis relation;
        private FilterAnalysis filter;
        private List<List<GroupByKey>> groupByKeys;
        private List<SortItemAnalysis> sortings;
        private boolean isSubqueryOrCte;

        public static Builder from(QueryAnalysis queryAnalysis)
        {
            Builder builder = new Builder();
            builder.selectItems.addAll(queryAnalysis.selectItems);
            builder.relation = queryAnalysis.relation;
            builder.filter = queryAnalysis.filter;
            builder.groupByKeys = queryAnalysis.groupByKeys;
            builder.sortings = queryAnalysis.sortings;
            builder.isSubqueryOrCte = queryAnalysis.isSubqueryOrCte;
            return builder;
        }

        public Builder addSelectItem(ColumnAnalysis selectItem)
        {
            selectItems.add(selectItem);
            return this;
        }

        public Builder setRelation(RelationAnalysis relation)
        {
            this.relation = relation;
            return this;
        }

        public Builder setFilter(FilterAnalysis filter)
        {
            this.filter = filter;
            return this;
        }

        public Builder setGroupByKeys(List<List<GroupByKey>> groupByKeys)
        {
            this.groupByKeys = groupByKeys;
            return this;
        }

        public Builder setSortings(List<SortItemAnalysis> sortings)
        {
            this.sortings = sortings;
            return this;
        }

        public Builder setSubqueryOrCte(boolean subqueryOrCte)
        {
            isSubqueryOrCte = subqueryOrCte;
            return this;
        }

        public List<ColumnAnalysis> getSelectItems()
        {
            return selectItems;
        }

        public QueryAnalysis build()
        {
            return new QueryAnalysis(selectItems, relation, filter, groupByKeys, sortings, isSubqueryOrCte);
        }
    }
}
