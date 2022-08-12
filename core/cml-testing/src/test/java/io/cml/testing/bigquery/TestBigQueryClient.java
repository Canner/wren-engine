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

import com.google.cloud.bigquery.Table;
import io.cml.connector.bigquery.BigQueryClient;
import io.cml.connector.bigquery.BigQueryConfig;
import io.cml.metadata.TableHandle;
import io.cml.server.module.BigQueryConnectorModule;
import io.cml.spi.metadata.CatalogName;
import io.cml.spi.metadata.SchemaTableName;
import org.testng.annotations.Test;

import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryClient
{
    @Test
    public void testCacheMV()
    {
        BigQueryClient client = bigQueryClient();
        Table table = client.getTable(
                new TableHandle(
                        new CatalogName("canner-cml"),
                        new SchemaTableName("canner_cube", "gby_rflag_lstatus_sdate")));
        assertThat(table).isNotNull();
        assertThat(client.getCacheMV(table.getTableId())).isEqualTo(table);
    }

    private static BigQueryClient bigQueryClient()
    {
        BigQueryConfig config = new BigQueryConfig();
        config.setProjectId(getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .setParentProjectId(getenv("TEST_BIG_QUERY_PARENT_PROJECT_ID"))
                .setCredentialsKey(getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .setLocation("asia-east1");

        return BigQueryConnectorModule.provideBigQuery(
                config,
                BigQueryConnectorModule.createHeaderProvider(),
                BigQueryConnectorModule.provideBigQueryCredentialsSupplier(config));
    }
}
