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

package io.wren.main.web.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryAnalysisDto
{
    private List<ColumnAnalysisDto> selectItems;
    private RelationAnalysisDto relation;
    private FilterAnalysisDto filter;
    private List<List<GroupByKeyDto>> groupByKeys;
    private List<SortItemAnalysisDto> sortings;
    private boolean isSubqueryOrCte;

    @JsonCreator
    public QueryAnalysisDto(
            List<ColumnAnalysisDto> selectItems,
            RelationAnalysisDto relation,
            FilterAnalysisDto filter,
            List<List<GroupByKeyDto>> groupByKeys,
            List<SortItemAnalysisDto> sortings,
            boolean isSubqueryOrCte)
    {
        this.selectItems = selectItems;
        this.relation = relation;
        this.filter = filter;
        this.groupByKeys = groupByKeys;
        this.sortings = sortings;
        this.isSubqueryOrCte = isSubqueryOrCte;
    }

    @JsonProperty
    public List<ColumnAnalysisDto> getSelectItems()
    {
        return selectItems;
    }

    @JsonProperty
    public RelationAnalysisDto getRelation()
    {
        return relation;
    }

    @JsonProperty
    public FilterAnalysisDto getFilter()
    {
        return filter;
    }

    @JsonProperty
    public List<List<GroupByKeyDto>> getGroupByKeys()
    {
        return groupByKeys;
    }

    @JsonProperty
    public List<SortItemAnalysisDto> getSortings()
    {
        return sortings;
    }

    @JsonProperty("isSubqueryOrCte")
    public boolean isSubqueryOrCte()
    {
        return isSubqueryOrCte;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ColumnAnalysisDto
    {
        private Optional<String> alias;
        private String expression;
        private Map<String, String> properties;
        private NodeLocationDto nodeLocation;
        private List<ExprSourceDto> exprSources;

        @JsonCreator
        public ColumnAnalysisDto(Optional<String> alias, String expression, Map<String, String> properties, NodeLocationDto nodeLocation, List<ExprSourceDto> exprSources)
        {
            this.alias = alias;
            this.expression = expression;
            this.properties = properties;
            this.nodeLocation = nodeLocation;
            this.exprSources = exprSources;
        }

        @JsonProperty
        public String getExpression()
        {
            return expression;
        }

        @JsonProperty
        public Optional<String> getAlias()
        {
            return alias;
        }

        @JsonProperty
        public Map<String, String> getProperties()
        {
            return properties;
        }

        @JsonProperty
        public NodeLocationDto getNodeLocation()
        {
            return nodeLocation;
        }

        @JsonProperty
        public List<ExprSourceDto> getExprSources()
        {
            return exprSources;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RelationAnalysisDto
    {
        private String type;
        private String alias;
        private RelationAnalysisDto left;
        private RelationAnalysisDto right;
        private JoinCriteriaDto criteria;
        private String tableName;
        private List<QueryAnalysisDto> body;
        private List<ExprSourceDto> exprSources;
        private NodeLocationDto nodeLocation;

        @JsonCreator
        public RelationAnalysisDto(
                String type,
                String alias,
                RelationAnalysisDto left,
                RelationAnalysisDto right,
                JoinCriteriaDto criteria,
                String tableName,
                List<QueryAnalysisDto> body,
                List<ExprSourceDto> exprSources,
                NodeLocationDto nodeLocation)
        {
            this.type = type;
            this.alias = alias;
            this.left = left;
            this.right = right;
            this.criteria = criteria;
            this.tableName = tableName;
            this.body = body;
            this.exprSources = exprSources;
            this.nodeLocation = nodeLocation;
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }

        @JsonProperty
        public String getAlias()
        {
            return alias;
        }

        @JsonProperty
        public RelationAnalysisDto getLeft()
        {
            return left;
        }

        @JsonProperty
        public RelationAnalysisDto getRight()
        {
            return right;
        }

        @JsonProperty
        public JoinCriteriaDto getCriteria()
        {
            return criteria;
        }

        @JsonProperty
        public String getTableName()
        {
            return tableName;
        }

        @JsonProperty
        public List<QueryAnalysisDto> getBody()
        {
            return body;
        }

        @JsonProperty
        public List<ExprSourceDto> getExprSources()
        {
            return exprSources;
        }

        @JsonProperty
        public NodeLocationDto getNodeLocation()
        {
            return nodeLocation;
        }
    }

    public static class JoinCriteriaDto
    {
        private String expression;
        private NodeLocationDto nodeLocation;

        @JsonCreator
        public JoinCriteriaDto(String expression, NodeLocationDto nodeLocation)
        {
            this.expression = expression;
            this.nodeLocation = nodeLocation;
        }

        @JsonProperty
        public String getExpression()
        {
            return expression;
        }

        @JsonProperty
        public NodeLocationDto getNodeLocation()
        {
            return nodeLocation;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class FilterAnalysisDto
    {
        private String type;
        private FilterAnalysisDto left;
        private FilterAnalysisDto right;
        private String node;
        private NodeLocationDto nodeLocation;
        private List<ExprSourceDto> exprSources;

        @JsonCreator
        public FilterAnalysisDto(String type, FilterAnalysisDto left, FilterAnalysisDto right, String node, NodeLocationDto nodeLocation, List<ExprSourceDto> exprSources)
        {
            this.type = type;
            this.left = left;
            this.right = right;
            this.node = node;
            this.nodeLocation = nodeLocation;
            this.exprSources = exprSources;
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }

        @JsonProperty
        public FilterAnalysisDto getLeft()
        {
            return left;
        }

        @JsonProperty
        public FilterAnalysisDto getRight()
        {
            return right;
        }

        @JsonProperty
        public String getNode()
        {
            return node;
        }

        @JsonProperty
        public NodeLocationDto getNodeLocation()
        {
            return nodeLocation;
        }

        @JsonProperty
        public List<ExprSourceDto> getExprSources()
        {
            return exprSources;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SortItemAnalysisDto
    {
        private String expression;
        private String ordering;
        private NodeLocationDto nodeLocation;
        private List<ExprSourceDto> exprSources;

        @JsonCreator
        public SortItemAnalysisDto(String expression, String ordering, NodeLocationDto nodeLocation, List<ExprSourceDto> exprSources)
        {
            this.expression = expression;
            this.ordering = ordering;
            this.nodeLocation = nodeLocation;
            this.exprSources = exprSources;
        }

        @JsonProperty
        public String getExpression()
        {
            return expression;
        }

        @JsonProperty
        public String getOrdering()
        {
            return ordering;
        }

        @JsonProperty
        public NodeLocationDto getNodeLocation()
        {
            return nodeLocation;
        }

        @JsonProperty
        public List<ExprSourceDto> getExprSources()
        {
            return exprSources;
        }
    }

    public static class ExprSourceDto
    {
        private String expression;
        private String sourceDataset;
        private String sourceColumn;
        private NodeLocationDto nodeLocation;

        @JsonCreator
        public ExprSourceDto(String expression, String sourceDataset, String sourceColumn, NodeLocationDto nodeLocation)
        {
            this.expression = expression;
            this.sourceDataset = sourceDataset;
            this.sourceColumn = sourceColumn;
            this.nodeLocation = nodeLocation;
        }

        @JsonProperty
        public String getExpression()
        {
            return expression;
        }

        @JsonProperty
        public String getSourceDataset()
        {
            return sourceDataset;
        }

        @JsonProperty
        public String getSourceColumn()
        {
            return sourceColumn;
        }

        @JsonProperty
        public NodeLocationDto getNodeLocation()
        {
            return nodeLocation;
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
            ExprSourceDto that = (ExprSourceDto) o;
            return Objects.equals(expression, that.expression) &&
                    Objects.equals(sourceDataset, that.sourceDataset) &&
                    Objects.equals(sourceColumn, that.sourceColumn) &&
                    Objects.equals(nodeLocation, that.nodeLocation);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expression, sourceDataset, sourceColumn, nodeLocation);
        }

        @Override
        public String toString()
        {
            return "ExprSourceDto{" +
                    "expression='" + expression + '\'' +
                    ", sourceDataset='" + sourceDataset + '\'' +
                    ", sourceColumn='" + sourceColumn + '\'' +
                    ", nodeLocation=" + nodeLocation +
                    '}';
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class GroupByKeyDto
    {
        private String expression;
        private NodeLocationDto nodeLocation;
        private List<ExprSourceDto> exprSources;

        @JsonCreator
        public GroupByKeyDto(String expression, NodeLocationDto nodeLocation, List<ExprSourceDto> exprSources)
        {
            this.expression = expression;
            this.nodeLocation = nodeLocation;
            this.exprSources = exprSources;
        }

        @JsonProperty
        public String getExpression()
        {
            return expression;
        }

        @JsonProperty
        public NodeLocationDto getNodeLocation()
        {
            return nodeLocation;
        }

        @JsonProperty
        public List<ExprSourceDto> getExprSources()
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
            GroupByKeyDto that = (GroupByKeyDto) o;
            return Objects.equals(expression, that.expression) &&
                    Objects.equals(nodeLocation, that.nodeLocation)
                    && Objects.equals(exprSources, that.exprSources);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expression, nodeLocation, exprSources);
        }

        @Override
        public String toString()
        {
            return "GroupByKeyDto{" +
                    "expression='" + expression + '\'' +
                    ", nodeLocation=" + nodeLocation +
                    ", exprSources=" + exprSources +
                    '}';
        }
    }
}
