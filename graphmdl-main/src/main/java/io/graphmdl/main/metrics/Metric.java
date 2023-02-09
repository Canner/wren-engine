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

package io.graphmdl.main.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Metric
{
    private final String name;
    private final String source;
    private final Type type;
    private final String sql;
    private final Set<String> dimensions;
    private final String timestamp;
    private final Set<TimeGrain> timeGrains;
    // Filters are combined using AND clauses
    private final Set<Filter> filters;

    @JsonCreator
    public Metric(
            @JsonProperty("name") String name,
            @JsonProperty("source") String source,
            @JsonProperty("type") Type type,
            @JsonProperty("sql") String sql,
            @JsonProperty("dimensions") Set<String> dimensions,
            @JsonProperty("timestamp") String timestamp,
            @JsonProperty("timeGrains") Set<TimeGrain> timeGrains,
            @JsonProperty("filters") Set<Filter> filters)
    {
        this.name = requireNonNull(name);
        this.source = requireNonNull(source);
        this.type = requireNonNull(type);
        this.sql = requireNonNull(sql);
        this.dimensions = requireNonNull(dimensions);
        this.timestamp = requireNonNull(timestamp);
        this.timeGrains = requireNonNull(timeGrains);
        this.filters = requireNonNull(filters);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getSource()
    {
        return source;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getSql()
    {
        return sql;
    }

    @JsonProperty
    public Set<String> getDimensions()
    {
        return dimensions;
    }

    @JsonProperty
    public String getTimestamp()
    {
        return timestamp;
    }

    @JsonProperty
    public Set<TimeGrain> getTimeGrains()
    {
        return timeGrains;
    }

    @JsonProperty
    public Set<Filter> getFilters()
    {
        return filters;
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
        Metric metric = (Metric) o;

        return Objects.equals(name, metric.name)
                && Objects.equals(source, metric.source)
                && type == metric.type
                && Objects.equals(sql, metric.sql)
                && Objects.equals(dimensions, metric.dimensions)
                && Objects.equals(timestamp, metric.timestamp)
                && Objects.equals(timeGrains, metric.timeGrains)
                && Objects.equals(filters, metric.filters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, source, type, sql, dimensions, timestamp, timeGrains, filters);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("source", source)
                .add("type", type)
                .add("sql", sql)
                .add("dimensions", dimensions)
                .add("timestamp", timestamp)
                .add("timeGrains", timeGrains)
                .add("filters", filters)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Metric metric)
    {
        return builder()
                .setName(metric.getName())
                .setSource(metric.getSource())
                .setType(metric.getType())
                .setDimensions(metric.getDimensions())
                .setFilters(metric.getFilters())
                .setTimestamp(metric.getTimestamp())
                .setTimeGrains(metric.getTimeGrains())
                .setSql(metric.getSql());
    }

    public static class Builder
    {
        private String name;
        private String source;
        private Type type;
        private String sql;
        private Set<String> dimensions = Set.of();
        private String timestamp;
        private Set<TimeGrain> timeGrains;
        private Set<Filter> filters = Set.of();

        private Builder() {}

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder setSource(String source)
        {
            this.source = source;
            return this;
        }

        public Builder setType(Type type)
        {
            this.type = type;
            return this;
        }

        public Builder setSql(String sql)
        {
            this.sql = sql;
            return this;
        }

        public Builder setDimensions(Set<String> dimensions)
        {
            this.dimensions = dimensions;
            return this;
        }

        public Builder setTimestamp(String timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        public Builder setTimeGrains(Set<TimeGrain> timeGrains)
        {
            this.timeGrains = timeGrains;
            return this;
        }

        public Builder setFilters(Set<Filter> filters)
        {
            this.filters = filters;
            return this;
        }

        public Metric build()
        {
            return new Metric(name, source, type, sql, dimensions, timestamp, timeGrains, filters);
        }
    }

    public enum TimeGrain
    {
        DAY(List.of("YEAR", "MONTH", "DAY")),
        WEEK(List.of("YEAR", "WEEK")),
        QUARTER(List.of("YEAR", "QUARTER")),
        MONTH(List.of("YEAR", "MONTH")),
        YEAR(List.of("YEAR"));

        private final List<String> groupUnits;

        TimeGrain(List<String> groupUnits)
        {
            this.groupUnits = requireNonNull(groupUnits);
        }

        public List<String> getGroupUnits()
        {
            return groupUnits;
        }
    }

    public enum Type
    {
        COUNT,
        COUNT_DISTINCT,
        SUM,
        AVG,
        MIN,
        MAX,
    }

    public static class Filter
    {
        private final String field;
        private final Operator operator;
        private final String value;

        @JsonCreator
        public Filter(
                @JsonProperty("field") String field,
                @JsonProperty("operator") Operator operator,
                @JsonProperty("value") String value)
        {
            this.field = field;
            this.operator = operator;
            this.value = value;
        }

        @JsonProperty
        public String getField()
        {
            return field;
        }

        @JsonProperty
        public Operator getOperator()
        {
            return operator;
        }

        @JsonProperty
        public String getValue()
        {
            return value;
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
            Filter filter = (Filter) o;
            return Objects.equals(field, filter.field) && operator == filter.operator && Objects.equals(value, filter.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(field, operator, value);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("field", field)
                    .add("operator", operator)
                    .add("value", value)
                    .toString();
        }

        public enum Operator
        {
            IS("is"),
            GREATER_THAN(">"),
            GREATER_THAN_OR_EQUAL_To(">="),
            LESS_THAN("<"),
            LESS_THAN_OR_EQUAL_TO("<="),
            EQUAL_TO("="),
            NOT_EQUAL_TO("!=");
            private final String symbol;

            Operator(String symbol)
            {
                this.symbol = requireNonNull(symbol);
            }

            public String getSymbol()
            {
                return symbol;
            }
        }
    }
}
