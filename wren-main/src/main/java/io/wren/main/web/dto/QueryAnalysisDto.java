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
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryAnalysisDto
{
    private List<ColumnAnalysisDto> selectItems;
    private RelationAnalysisDto relation;
    private FilterAnalysisDto filter;
    private List<List<String>> groupByKeys;
    private List<SortItemAnalysisDto> sortings;

    @JsonCreator
    public QueryAnalysisDto(List<ColumnAnalysisDto> selectItems, RelationAnalysisDto relation, FilterAnalysisDto filter, List<List<String>> groupByKeys, List<SortItemAnalysisDto> sortings)
    {
        this.selectItems = selectItems;
        this.relation = relation;
        this.filter = filter;
        this.groupByKeys = groupByKeys;
        this.sortings = sortings;
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
    public List<List<String>> getGroupByKeys()
    {
        return groupByKeys;
    }

    @JsonProperty
    public List<SortItemAnalysisDto> getSortings()
    {
        return sortings;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ColumnAnalysisDto
    {
        private Optional<String> alias;
        private String expression;
        private Map<String, String> properties;

        @JsonCreator
        public ColumnAnalysisDto(Optional<String> alias, String expression, Map<String, String> properties)
        {
            this.alias = alias;
            this.expression = expression;
            this.properties = properties;
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
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RelationAnalysisDto
    {
        private String type;
        private String alias;
        private RelationAnalysisDto left;
        private RelationAnalysisDto right;
        private String criteria;
        private String tableName;
        private List<QueryAnalysisDto> body;

        @JsonCreator
        public RelationAnalysisDto(String type, String alias, RelationAnalysisDto left, RelationAnalysisDto right, String criteria, String tableName, List<QueryAnalysisDto> body)
        {
            this.type = type;
            this.alias = alias;
            this.left = left;
            this.right = right;
            this.criteria = criteria;
            this.tableName = tableName;
            this.body = body;
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
        public String getCriteria()
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
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class FilterAnalysisDto
    {
        private String type;
        private FilterAnalysisDto left;
        private FilterAnalysisDto right;
        private String node;

        @JsonCreator
        public FilterAnalysisDto(String type, FilterAnalysisDto left, FilterAnalysisDto right, String node)
        {
            this.type = type;
            this.left = left;
            this.right = right;
            this.node = node;
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
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SortItemAnalysisDto
    {
        private String expression;
        private String ordering;

        @JsonCreator
        public SortItemAnalysisDto(String expression, String ordering)
        {
            this.expression = expression;
            this.ordering = ordering;
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
    }
}
