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

package io.cml.metrics;

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

    private Metric(String name, String source, Type type, String sql, Set<String> dimensions, String timestamp, Set<TimeGrain> timeGrains, Set<Filter> filters)
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

    public String getName()
    {
        return name;
    }

    public String getSource()
    {
        return source;
    }

    public Type getType()
    {
        return type;
    }

    public String getSql()
    {
        return sql;
    }

    public Set<String> getDimensions()
    {
        return dimensions;
    }

    public String getTimestamp()
    {
        return timestamp;
    }

    public Set<TimeGrain> getTimeGrains()
    {
        return timeGrains;
    }

    public Set<Filter> getFilters()
    {
        return filters;
    }

    public static Builder builder()
    {
        return new Builder();
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

        public Filter(String field, Operator operator, String value)
        {
            this.field = field;
            this.operator = operator;
            this.value = value;
        }

        public String getField()
        {
            return field;
        }

        public Operator getOperator()
        {
            return operator;
        }

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
