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

package io.cml.testing.canner;

import com.google.common.collect.ImmutableMap;
import io.cml.server.module.CannerConnectorModule;
import io.cml.testing.TestingCmlServer;

import static io.cml.metrics.MetricConfig.METRIC_ROOT_PATH;
import static java.lang.System.getenv;

public interface CannerTesting
{
    default TestingCmlServer createCmlServerWithCanner()
    {
        return TestingCmlServer.builder()
                .setRequiredConfigs(
                        ImmutableMap.<String, String>builder()
                                .put("canner.pat", getenv("TEST_CANNER_PAT"))
                                .put("canner.url", getenv("TEST_CANNER_URL"))
                                .put("canner.availableWorkspace", getenv("TEST_CANNER_AVAILABLE_WORKSPACE"))
                                .put(METRIC_ROOT_PATH, "ignored")
                                .build())
                .addAdditionalModule(new CannerConnectorModule())
                .build();
    }
}
