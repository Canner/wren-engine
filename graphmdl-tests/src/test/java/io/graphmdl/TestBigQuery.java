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

package io.graphmdl;

import io.graphmdl.base.SessionContext;
import io.graphmdl.main.connector.bigquery.BigQueryClient;
import io.graphmdl.main.connector.bigquery.BigQueryConfig;
import io.graphmdl.main.connector.bigquery.BigQueryMetadata;
import io.graphmdl.main.connector.bigquery.BigQuerySqlConverter;
import io.graphmdl.main.server.module.BigQueryConnectorModule;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQuery
{
    private BigQueryClient bigQueryClient;
    private BigQueryMetadata bigQueryMetadata;
    private BigQuerySqlConverter bigQuerySqlConverter;

    @BeforeClass
    public void createBigQueryClient()
    {
        BigQueryConfig config = new BigQueryConfig();
        config.setProjectId(getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .setParentProjectId(getenv("TEST_BIG_QUERY_PARENT_PROJECT_ID"))
                .setCredentialsKey(getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .setLocation("asia-east1");

        bigQueryClient = BigQueryConnectorModule.provideBigQuery(
                config,
                BigQueryConnectorModule.createHeaderProvider(),
                BigQueryConnectorModule.provideBigQueryCredentialsSupplier(config));

        bigQueryMetadata = new BigQueryMetadata(bigQueryClient, config);
        bigQuerySqlConverter = new BigQuerySqlConverter(bigQueryMetadata);
    }

    @Test
    public void testBigQueryGroupByOrdinal()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT o_custkey, COUNT(*) AS cnt\n" +
                        "FROM \"canner-cml\".\"tpch_tiny\".\"orders\"\n" +
                        "GROUP BY 1", SessionContext.builder().build()))
                .isEqualTo("SELECT o_custkey, COUNT(*) AS cnt\n" +
                        "FROM `canner-cml`.tpch_tiny.orders\n" +
                        "GROUP BY o_custkey");
    }

    @Test
    public void testCaseSensitive()
    {
        assertThat(bigQuerySqlConverter.convert("SELECT a FROM \"canner-cml\".\"cml_temp\".\"canner\"", SessionContext.builder().build()))
                .isEqualTo("SELECT a\n" +
                        "FROM `canner-cml`.cml_temp.canner");
        assertThat(bigQuerySqlConverter.convert("SELECT b FROM \"canner-cml\".\"cml_temp\".\"CANNER\"", SessionContext.builder().build()))
                .isEqualTo("SELECT b\n" +
                        "FROM `canner-cml`.cml_temp.CANNER");
    }
}
