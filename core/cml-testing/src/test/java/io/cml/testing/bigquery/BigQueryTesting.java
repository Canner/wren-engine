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

package io.cml.testing.bigquery;

import com.google.common.collect.ImmutableMap;
import io.cml.server.module.BigQueryConnectorModule;
import io.cml.testing.TestingCmlServer;

import static io.cml.metrics.MetricConfig.METRIC_ROOT_PATH;
import static java.lang.System.getenv;

public interface BigQueryTesting
{
    default TestingCmlServer createCmlServerWithBigQuery()
    {
        return TestingCmlServer.builder()
                .setRequiredConfigs(
                        ImmutableMap.<String, String>builder()
                                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                                .put("bigquery.location", "asia-east1")
                                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                                .put(METRIC_ROOT_PATH, getenv("TEST_CML_FILE_METRIC_STORE_HOME"))
                                .build())
                .addAdditionalModule(new BigQueryConnectorModule())
                .build();
    }
}
