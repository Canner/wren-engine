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

package io.wren.base.sqlrewrite.analyzer;

import io.wren.base.dto.Metric;
import io.wren.base.dto.TimeGrain;
import io.wren.base.dto.TimeUnit;

import static java.util.Objects.requireNonNull;

public class MetricRollupInfo
{
    private final Metric metric;
    private final TimeGrain timeGrain;
    private final TimeUnit timeUnit;

    public MetricRollupInfo(Metric metric, TimeGrain timeGrain, TimeUnit timeUnit)
    {
        this.metric = requireNonNull(metric, "metric is null");
        this.timeGrain = requireNonNull(timeGrain, "timeGrain is null");
        this.timeUnit = requireNonNull(timeUnit, "datePart is null");
    }

    public Metric getMetric()
    {
        return metric;
    }

    public TimeGrain getTimeGrain()
    {
        return timeGrain;
    }

    public TimeUnit getDatePart()
    {
        return timeUnit;
    }
}
