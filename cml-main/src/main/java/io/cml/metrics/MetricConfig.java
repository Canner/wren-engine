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

import io.airlift.configuration.Config;

public class MetricConfig
{
    public static final String METRIC_ROOT_PATH = "metric.rootPath";
    private String rootPath;

    public String getRootPath()
    {
        return rootPath;
    }

    @Config(METRIC_ROOT_PATH)
    public MetricConfig setRootPath(String rootPath)
    {
        this.rootPath = rootPath;
        return this;
    }
}
