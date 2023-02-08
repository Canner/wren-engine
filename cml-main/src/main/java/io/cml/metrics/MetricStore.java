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
import java.util.Optional;

public interface MetricStore
{
    /**
     * @return all created metrics in the root
     */
    List<Metric> listMetrics();

    /**
     * @return the full information of the specified metric or empty if not found
     */
    Optional<Metric> getMetric(String metricName);

    /**
     * Create the specified metric
     */
    void createMetric(Metric metric);

    /**
     * Drop the specified metric
     */
    void dropMetric(String metricName);

    /**
     * Create the specified metric
     */
    void createMetricSql(MetricSql metricSql);

    /**
     * @return the list of MetricSqls generated from the specified metric
     */
    List<MetricSql> listMetricSqls(String metricName);

    /**
     * @return the full information of the specified metric sql or empty if not found
     */
    Optional<MetricSql> getMetricSql(String metric, String metricSqlName);
}
