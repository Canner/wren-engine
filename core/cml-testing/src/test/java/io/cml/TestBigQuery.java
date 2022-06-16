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

package io.cml;

import io.cml.connector.bigquery.BigQueryClient;
import io.cml.connector.bigquery.BigQueryConfig;
import io.cml.connector.bigquery.BigQueryMetadata;
import io.cml.connector.bigquery.BigQuerySqlConverter;
import io.cml.server.module.BigQueryConnectorModule;
import org.testng.annotations.Test;

import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQuery
{
    @Test
    public void testBigQueryGetTables()
    {
        BigQueryConfig config = new BigQueryConfig();
        config.setProjectId(getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .setParentProjectId(getenv("TEST_BIG_QUERY_PARENT_PROJECT_ID"))
                .setCredentialsKey(getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .setLocation("US");

        BigQueryClient bigQueryClient = BigQueryConnectorModule.provideBigQuery(
                config,
                BigQueryConnectorModule.createHeaderProvider(),
                BigQueryConnectorModule.provideBigQueryCredentialsSupplier(config));

        BigQueryMetadata bigQueryMetadata = new BigQueryMetadata(bigQueryClient);
        BigQuerySqlConverter bigQuerySqlConverter = new BigQuerySqlConverter(bigQueryMetadata);

        String output = bigQuerySqlConverter.convert("SELECT o_custkey, COUNT(*) as cnt FROM \"cannerflow-286003\".\"tpch_tiny\".\"orders\" GROUP BY o_custkey");
        assertThat(output).isEqualTo("SELECT o_custkey, CAST(cnt AS INT64) AS cnt\n" +
                "FROM `cannerflow-286003`.cml_temp.mv_orders_group_by_test");
    }
}
