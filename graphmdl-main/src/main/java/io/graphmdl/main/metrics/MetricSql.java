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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.graphmdl.main.connector.bigquery.BigQueryMetadata;
import io.graphmdl.spi.metadata.SchemaTableName;
import io.graphmdl.sqlrewrite.Utils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
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

    private final Status status;

    private final String errorMessage;

    @JsonCreator
    public MetricSql(
            @JsonProperty("baseMetricName") String baseMetricName,
            @JsonProperty("name") String name,
            @JsonProperty("source") String source,
            @JsonProperty("type") Metric.Type type,
            @JsonProperty("sql") String sql,
            @JsonProperty("dimensions") List<String> dimensions,
            @JsonProperty("timestamp") String timestamp,
            @JsonProperty("timeGrain") Metric.TimeGrain timeGrain,
            @JsonProperty("filters") List<Metric.Filter> filters,
            @JsonProperty("status") Status status,
            @JsonProperty("errorMessage") @Nullable String errorMessage)
    {
        this.baseMetricName = requireNonNull(baseMetricName, "baseMetricName is null");
        this.name = requireNonNull(name, "name is null");
        this.source = requireNonNull(source, "source is null");
        this.type = requireNonNull(type, "type is null");
        this.sql = requireNonNull(sql, "sql is null");
        this.dimensions = requireNonNull(dimensions, "dimensions is null");
        this.timestamp = requireNonNull(timestamp, "timestamp is null");
        this.timeGrain = requireNonNull(timeGrain, "timeGrain is null");
        this.filters = requireNonNull(filters, "filters is null");
        this.status = requireNonNull(status, "status is null");
        this.errorMessage = errorMessage;
    }

    public static List<MetricSql> of(Metric metric)
    {
        Builder metricSql = builder()
                .setBaseMetricName(metric.getName())
                .setSource(metric.getSource())
                .setType(metric.getType())
                .setSql(metric.getSql())
                .setTimestamp(metric.getTimestamp());

        List<Builder> metricSqls = expandDimensions(List.of(metricSql), List.copyOf(metric.getDimensions()));
        metricSqls = expandTimeGrains(metricSqls, List.copyOf(metric.getTimeGrains()));
        metricSqls = expandFilters(metricSqls, List.copyOf(metric.getFilters()));
        return metricSqls.stream().map(Builder::build).collect(toList());
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getBaseMetricName()
    {
        return baseMetricName;
    }

    @JsonProperty
    public String getSource()
    {
        return source;
    }

    @JsonProperty
    public Metric.Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getSql()
    {
        return sql;
    }

    @JsonProperty
    public List<String> getDimensions()
    {
        return dimensions;
    }

    @JsonProperty
    public String getTimestamp()
    {
        return timestamp;
    }

    @JsonProperty
    public Metric.TimeGrain getTimeGrain()
    {
        return timeGrain;
    }

    @JsonProperty
    public List<Metric.Filter> getFilters()
    {
        return filters;
    }

    @JsonProperty
    public Status getStatus()
    {
        return status;
    }

    @Nullable
    @JsonProperty
    public String getErrorMessage()
    {
        return errorMessage;
    }

    /**
     * Get sql generated by MetricSql.
     * Note that we use pg sql syntax here to generate metric SQL, so when
     * we need to create table for this MetricSql, we need to convert it to the responding
     * sql dialect. more info, see {@link BigQueryMetadata#createMaterializedView(SchemaTableName, String)}
     *
     * @return sql
     */
    public String sql()
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
        return format("SELECT %s, %s FROM %s WHERE %s GROUP BY %s",
                selectItems, typeAgg, source, whereExpression, groupByExpression);
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
        return Objects.equals(baseMetricName, metricSql.baseMetricName)
                && Objects.equals(name, metricSql.name)
                && Objects.equals(source, metricSql.source)
                && type == metricSql.type
                && Objects.equals(sql, metricSql.sql)
                && Objects.equals(dimensions, metricSql.dimensions)
                && Objects.equals(timestamp, metricSql.timestamp)
                && timeGrain == metricSql.timeGrain
                && Objects.equals(filters, metricSql.filters)
                && Objects.equals(status, metricSql.status)
                && Objects.equals(errorMessage, metricSql.errorMessage);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                baseMetricName,
                name,
                source,
                type,
                sql,
                dimensions,
                timestamp,
                timeGrain,
                filters,
                status,
                errorMessage);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("baseMetricName", baseMetricName)
                .add("name", name)
                .add("source", source)
                .add("type", type)
                .add("sql", sql)
                .add("dimensions", dimensions)
                .add("timestamp", timestamp)
                .add("timeGrain", timeGrain)
                .add("filters", filters)
                .add("status", status)
                .add("errorMessage", errorMessage)
                .toString();
    }

    /**
     * Create builder from MetricSql
     */
    static Builder builder(MetricSql metricSql)
    {
        return new Builder()
                .setBaseMetricName(metricSql.getBaseMetricName())
                .setName(metricSql.getName())
                .setSource(metricSql.getSource())
                .setType(metricSql.getType())
                .setSql(metricSql.getSql())
                .setDimensions(metricSql.getDimensions())
                .setTimestamp(metricSql.getTimestamp())
                .setTimeGrain(metricSql.getTimeGrain())
                .setFilters(metricSql.getFilters())
                .setStatus(metricSql.getStatus())
                .setErrorMessage(metricSql.getErrorMessage());
    }

    /**
     * Create builder from builder
     */
    private static Builder builder(Builder builder)
    {
        return new Builder()
                .setBaseMetricName(builder.getBaseMetricName())
                .setName(builder.getName())
                .setSource(builder.getSource())
                .setType(builder.getType())
                .setSql(builder.getSql())
                .setDimensions(builder.getDimensions())
                .setTimestamp(builder.getTimestamp())
                .setTimeGrain(builder.getTimeGrain())
                .setFilters(builder.getFilters())
                .setStatus(builder.getStatus())
                .setErrorMessage(builder.getErrorMessage());
    }

    @VisibleForTesting
    static Builder builder()
    {
        return new Builder();
    }

    static class Builder
    {
        private String baseMetricName;
        private String name;
        private String source;
        private Metric.Type type;
        private String sql;
        private List<String> dimensions = List.of();
        private String timestamp;
        private Metric.TimeGrain timeGrain;
        private List<Metric.Filter> filters = List.of();
        private Status status = Status.PREPARING;
        private String errorMessage;

        public Builder setBaseMetricName(String baseMetricName)
        {
            this.baseMetricName = baseMetricName;
            return this;
        }

        public String getBaseMetricName()
        {
            return baseMetricName;
        }

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public String getName()
        {
            return name;
        }

        public Builder setSource(String source)
        {
            this.source = source;
            return this;
        }

        public String getSource()
        {
            return source;
        }

        public Builder setType(Metric.Type type)
        {
            this.type = type;
            return this;
        }

        public Metric.Type getType()
        {
            return type;
        }

        public Builder setSql(String sql)
        {
            this.sql = sql;
            return this;
        }

        public String getSql()
        {
            return sql;
        }

        public Builder setDimensions(List<String> dimensions)
        {
            this.dimensions = dimensions;
            return this;
        }

        public List<String> getDimensions()
        {
            return dimensions;
        }

        public Builder setTimestamp(String timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        public String getTimestamp()
        {
            return timestamp;
        }

        public Builder setTimeGrain(Metric.TimeGrain timeGrain)
        {
            this.timeGrain = timeGrain;
            return this;
        }

        public Metric.TimeGrain getTimeGrain()
        {
            return timeGrain;
        }

        public Builder setFilters(List<Metric.Filter> filters)
        {
            this.filters = filters;
            return this;
        }

        public List<Metric.Filter> getFilters()
        {
            return filters;
        }

        public Builder setStatus(Status status)
        {
            this.status = status;
            return this;
        }

        public Status getStatus()
        {
            return status;
        }

        public Builder setErrorMessage(String errorMessage)
        {
            this.errorMessage = errorMessage;
            return this;
        }

        public String getErrorMessage()
        {
            return errorMessage;
        }

        public MetricSql build()
        {
            return new MetricSql(
                    baseMetricName,
                    requireNonNullElseGet(name, () -> baseMetricName + "_" + Utils.randomTableSuffix()),
                    source,
                    type,
                    sql,
                    dimensions,
                    timestamp,
                    timeGrain,
                    filters,
                    status,
                    errorMessage);
        }
    }

    @VisibleForTesting
    static List<Builder> expandDimensions(List<Builder> metrics, List<String> dimensions)
    {
        checkArgument(metrics != null && !metrics.isEmpty(), "metrics is null or empty");
        List<List<String>> combinations = findCombinations(dimensions);
        ImmutableList.Builder<Builder> builder = ImmutableList.builder();
        for (Builder metric : metrics) {
            for (List<String> dims : combinations) {
                Builder newMetric = builder(metric);
                newMetric.setDimensions(dims);
                builder.add(newMetric);
            }
        }
        return builder.build();
    }

    @VisibleForTesting
    static List<Builder> expandTimeGrains(List<Builder> metrics, List<Metric.TimeGrain> timeGrains)
    {
        checkArgument(metrics != null && !metrics.isEmpty(), "metrics is null or empty");
        ImmutableList.Builder<Builder> builder = ImmutableList.builder();
        for (Builder metric : metrics) {
            for (Metric.TimeGrain timeGrain : timeGrains) {
                Builder newMetric = builder(metric);
                newMetric.setTimeGrain(timeGrain);
                builder.add(newMetric);
            }
        }
        return builder.build();
    }

    @VisibleForTesting
    static List<Builder> expandFilters(List<Builder> metrics, List<Metric.Filter> filters)
    {
        checkArgument(metrics != null && !metrics.isEmpty(), "metrics is null or empty");
        ImmutableList.Builder<Builder> builder = ImmutableList.builder();
        for (Builder metric : metrics) {
            for (List<Metric.Filter> filterList : findCombinations(filters)) {
                Builder newMetric = builder(metric);
                newMetric.setFilters(filterList);
                builder.add(newMetric);
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

    public enum Status
    {
        SUCCESS,
        FAILED,
        PREPARING,
    }
}
