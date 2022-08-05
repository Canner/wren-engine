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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.cml.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class MetricSql
{
    private final String baseMetricName;
    private final String name;
    private final String source;
    private final Metric.Type type;
    private final String sql;
    private final List<String> dimensions;

    private final String timestamp;
    private final Metric.TimeGrain timeGrain;
    private final List<Metric.Filter> filters;

    private MetricSql(String baseMetricName, String source, Metric.Type type, String sql, List<String> dimensions, String timestamp, Metric.TimeGrain timeGrain, List<Metric.Filter> filters)
    {
        this.baseMetricName = baseMetricName;
        this.name = baseMetricName + "_" + Utils.randomTableSuffix();
        this.source = source;
        this.type = type;
        this.sql = sql;
        this.dimensions = dimensions;
        this.timestamp = timestamp;
        this.timeGrain = timeGrain;
        this.filters = filters;
    }

    public static List<MetricSql> of(Metric metric)
    {
        MetricSql metricSql = new MetricSql(metric.getName(), metric.getSource(), metric.getType(), metric.getSql(), List.of(), metric.getTimestamp(), null, List.of());
        List<MetricSql> metricSqls = expandDimensions(List.of(metricSql), List.copyOf(metric.getDimensions()));
        metricSqls = expandTimeGrains(metricSqls, List.copyOf(metric.getTimeGrains()));
        metricSqls = expandFilters(metricSqls, List.copyOf(metric.getFilters()));
        return metricSqls;
    }

    public String name()
    {
        return name;
    }

    /**
     * Get CTAS sql for creating the metric table.
     *
     * @param schema that we wanted to place metric in
     * @return CTAS sql
     */
    public String sql(String schema)
    {
        // column name is necessary in each select element since metric is supposed to
        // be converted into table. In could service like bigquery, the select item should
        // have a name (i.e. column name)
        AtomicInteger colIdx = new AtomicInteger();
        Supplier<String> aliasName = () -> "_col" + colIdx.incrementAndGet();

        String timeGrainGroupBy = timeGrain.getGroupUnits().stream()
                .map(grain -> format("CAST(TRUNC(EXTRACT(%s FROM %s)) AS INTEGER) AS %s", grain, timestamp, aliasName.get()))
                .reduce((a, b) -> format("%s, %s", a, b))
                .orElse("");
        String typeAgg = format("%s(%s) AS %s", type, sql, aliasName.get());
        // no need to add alias to dimension since each dimension itself is a column name
        String dimsGroupBy = Joiner.on(",").join(dimensions);
        String selectItems = Joiner.on(",").join(List.of(dimsGroupBy, timeGrainGroupBy));
        String groupByExpression = Joiner.on(",").join(IntStream.rangeClosed(1, timeGrain.getGroupUnits().size() + dimensions.size()).boxed().collect(toList()));
        String whereExpression = filters.stream()
                .map(filter -> format("%s %s %s", filter.getField(), filter.getOperator().getSymbol(), filter.getValue()))
                .reduce((a, b) -> format("%s AND %s", a, b))
                .orElse("");
        return format("CREATE TABLE %s.%s AS SELECT %s, %s FROM %s WHERE %s GROUP BY %s",
                schema, name, selectItems, typeAgg, source, whereExpression, groupByExpression);
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
        MetricSql metricSql = (MetricSql) o;
        return Objects.equals(source, metricSql.source)
                && type == metricSql.type
                && Objects.equals(sql, metricSql.sql)
                && Objects.equals(dimensions, metricSql.dimensions)
                && Objects.equals(timestamp, metricSql.timestamp)
                && timeGrain == metricSql.timeGrain
                && Objects.equals(filters, metricSql.filters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, type, sql, dimensions, timestamp, timeGrain, filters);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", source)
                .add("type", type)
                .add("sql", sql)
                .add("dimensions", dimensions)
                .add("timestamp", timestamp)
                .add("timeGrain", timeGrain)
                .add("filters", filters)
                .toString();
    }

    private static Builder fromMetricSql(MetricSql metricSql)
    {
        return new Builder()
                .setBaseMetricName(metricSql.baseMetricName)
                .setSource(metricSql.source)
                .setType(metricSql.type)
                .setSql(metricSql.sql)
                .setDimensions(metricSql.dimensions)
                .setTimestamp(metricSql.timestamp)
                .setTimeGrains(metricSql.timeGrain)
                .setFilters(metricSql.filters);
    }

    @VisibleForTesting
    static Builder builder()
    {
        return new Builder();
    }

    @VisibleForTesting
    static class Builder
    {
        private String baseMetricName;
        private String source;
        private Metric.Type type;
        private String sql;
        private List<String> dimensions = List.of();
        private String timestamp;
        private Metric.TimeGrain timeGrains;
        private List<Metric.Filter> filters = List.of();

        public Builder setBaseMetricName(String baseMetricName)
        {
            this.baseMetricName = baseMetricName;
            return this;
        }

        public Builder setSource(String source)
        {
            this.source = source;
            return this;
        }

        public Builder setType(Metric.Type type)
        {
            this.type = type;
            return this;
        }

        public Builder setSql(String sql)
        {
            this.sql = sql;
            return this;
        }

        public Builder setDimensions(List<String> dimensions)
        {
            this.dimensions = dimensions;
            return this;
        }

        public Builder setTimestamp(String timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        public Builder setTimeGrains(Metric.TimeGrain timeGrains)
        {
            this.timeGrains = timeGrains;
            return this;
        }

        public Builder setFilters(List<Metric.Filter> filters)
        {
            this.filters = filters;
            return this;
        }

        public MetricSql build()
        {
            return new MetricSql(baseMetricName, source, type, sql, dimensions, timestamp, timeGrains, filters);
        }
    }

    @VisibleForTesting
    static List<MetricSql> expandDimensions(List<MetricSql> metrics, List<String> dimensions)
    {
        checkArgument(metrics != null && !metrics.isEmpty(), "metrics is null or empty");
        List<List<String>> combinations = findCombinations(dimensions);
        ImmutableList.Builder<MetricSql> builder = ImmutableList.builder();
        for (MetricSql metric : metrics) {
            for (List<String> dims : combinations) {
                MetricSql.Builder newMetric = fromMetricSql(metric);
                newMetric.setDimensions(dims);
                builder.add(newMetric.build());
            }
        }
        return builder.build();
    }

    @VisibleForTesting
    static List<MetricSql> expandTimeGrains(List<MetricSql> metrics, List<Metric.TimeGrain> timeGrains)
    {
        checkArgument(metrics != null && !metrics.isEmpty(), "metrics is null or empty");
        ImmutableList.Builder<MetricSql> builder = ImmutableList.builder();
        for (MetricSql metric : metrics) {
            for (Metric.TimeGrain timeGrain : timeGrains) {
                MetricSql.Builder newMetric = fromMetricSql(metric);
                newMetric.setTimeGrains(timeGrain);
                builder.add(newMetric.build());
            }
        }
        return builder.build();
    }

    @VisibleForTesting
    static List<MetricSql> expandFilters(List<MetricSql> metrics, List<Metric.Filter> filters)
    {
        checkArgument(metrics != null && !metrics.isEmpty(), "metrics is null or empty");
        ImmutableList.Builder<MetricSql> builder = ImmutableList.builder();
        for (MetricSql metric : metrics) {
            for (List<Metric.Filter> filterList : findCombinations(filters)) {
                MetricSql.Builder newMetric = fromMetricSql(metric);
                newMetric.setFilters(filterList);
                builder.add(newMetric.build());
            }
        }
        return builder.build();
    }

    /**
     * Find all combinations in this list
     *
     * @param elements we want to find combinations
     * @param <T> class
     * @return all combinations in elements
     */
    @VisibleForTesting
    static <T> List<List<T>> findCombinations(List<T> elements)
    {
        List<List<T>> result = new ArrayList<>();
        for (int i = 1; i <= elements.size(); i++) {
            findCombinations(result, List.copyOf(elements), new ArrayList<>(), 0, i);
        }
        return result;
    }

    private static <T> void findCombinations(List<List<T>> result, List<T> original, List<T> candidate, int start, int num)
    {
        if (candidate.size() == num) {
            result.add(List.copyOf(candidate));
            return;
        }

        for (int i = start; i < original.size(); i++) {
            candidate.add(original.get(i));
            findCombinations(result, original, candidate, i + 1, num);
            candidate.remove(candidate.size() - 1);
        }
    }
}
