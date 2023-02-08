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

package io.graphmdl.web.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.graphmdl.metrics.Metric;
import io.graphmdl.metrics.MetricSql;

import java.util.List;
import java.util.Set;

public final class MetricDto
{
    private String name;
    private String source;
    private Metric.Type type;
    private String sql;
    private Set<String> dimensions;
    private String timestamp;
    private Set<Metric.TimeGrain> timeGrains;
    private Set<Metric.Filter> filters;
    private List<MetricSql> metricSqls;

    public static MetricDto from(Metric metric, List<MetricSql> metricSqls)
    {
        return new MetricDto(
                metric.getName(),
                metric.getSource(),
                metric.getType(),
                metric.getSql(),
                metric.getDimensions(),
                metric.getTimestamp(),
                metric.getTimeGrains(),
                metric.getFilters(),
                metricSqls);
    }

    @JsonCreator
    public MetricDto(
            @JsonProperty("name") String name,
            @JsonProperty("source") String source,
            @JsonProperty("type") Metric.Type type,
            @JsonProperty("sql") String sql,
            @JsonProperty("dimensions") Set<String> dimensions,
            @JsonProperty("timestamp") String timestamp,
            @JsonProperty("timeGrains") Set<Metric.TimeGrain> timeGrains,
            @JsonProperty("filters") Set<Metric.Filter> filters,
            @JsonProperty("metricSqls") List<MetricSql> metricSqls)
    {
        this.name = name;
        this.source = source;
        this.type = type;
        this.sql = sql;
        this.dimensions = dimensions;
        this.timestamp = timestamp;
        this.timeGrains = timeGrains;
        this.filters = filters;
        this.metricSqls = metricSqls;
    }

    @JsonProperty("name")
    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    @JsonProperty("Source")
    public String getSource()
    {
        return source;
    }

    public void setSource(String source)
    {
        this.source = source;
    }

    @JsonProperty("type")
    public Metric.Type getType()
    {
        return type;
    }

    public void setType(Metric.Type type)
    {
        this.type = type;
    }

    @JsonProperty("sql")
    public String getSql()
    {
        return sql;
    }

    public void setSql(String sql)
    {
        this.sql = sql;
    }

    @JsonProperty("dimensions")
    public Set<String> getDimensions()
    {
        return dimensions;
    }

    public void setDimensions(Set<String> dimensions)
    {
        this.dimensions = dimensions;
    }

    @JsonProperty("timestamp")
    public String getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(String timestamp)
    {
        this.timestamp = timestamp;
    }

    @JsonProperty("timeGrains")
    public Set<Metric.TimeGrain> getTimeGrains()
    {
        return timeGrains;
    }

    public void setTimeGrains(Set<Metric.TimeGrain> timeGrains)
    {
        this.timeGrains = timeGrains;
    }

    @JsonProperty("filters")
    public Set<Metric.Filter> getFilters()
    {
        return filters;
    }

    public void setFilters(Set<Metric.Filter> filters)
    {
        this.filters = filters;
    }

    @JsonProperty("metricSqls")
    public List<MetricSql> getMetricSqls()
    {
        return metricSqls;
    }

    public void setMetricSqls(List<MetricSql> metricSqls)
    {
        this.metricSqls = metricSqls;
    }
}
