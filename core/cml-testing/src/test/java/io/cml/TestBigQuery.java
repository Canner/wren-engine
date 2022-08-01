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

import com.google.cloud.bigquery.MaterializedViewDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.cml.connector.bigquery.BigQueryClient;
import io.cml.connector.bigquery.BigQueryConfig;
import io.cml.connector.bigquery.BigQueryMetadata;
import io.cml.connector.bigquery.BigQuerySqlConverter;
import io.cml.server.module.BigQueryConnectorModule;
import io.cml.spi.SessionContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.cml.testing.Utils.randomTableSuffix;
import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQuery
{
    private BigQueryClient bigQueryClient;
    private BigQuerySqlConverter bigQuerySqlConverter;

    @BeforeClass
    public void createBigQueryClient()
    {
        BigQueryConfig config = new BigQueryConfig();
        config.setProjectId(getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .setParentProjectId(getenv("TEST_BIG_QUERY_PARENT_PROJECT_ID"))
                .setCredentialsKey(getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .setLocation("US");

        bigQueryClient = BigQueryConnectorModule.provideBigQuery(
                config,
                BigQueryConnectorModule.createHeaderProvider(),
                BigQueryConnectorModule.provideBigQueryCredentialsSupplier(config));

        BigQueryMetadata bigQueryMetadata = new BigQueryMetadata(bigQueryClient, config);
        bigQuerySqlConverter = new BigQuerySqlConverter(bigQueryMetadata);
    }

    @Test
    public void testBigQueryMVReplace()
    {
        String tableName = "mv_orders_group_by" + randomTableSuffix();
        TableId tableId = TableId.of("cml_temp", tableName);
        TableDefinition tableDefinition = MaterializedViewDefinition.of(
                "SELECT o_custkey, COUNT(*) as cnt\n" +
                        "FROM cannerflow-286003.tpch_tiny.orders\n" +
                        "GROUP BY o_custkey");
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        try {
            bigQueryClient.createTable(tableInfo);

            String output = bigQuerySqlConverter.convert(
                    "SELECT o_custkey, COUNT(*) as cnt\n" +
                            "FROM \"cannerflow-286003\".\"tpch_tiny\".\"orders\"\n" +
                            "GROUP BY o_custkey", SessionContext.builder().build());
            assertThat(output).isEqualTo("SELECT o_custkey, CAST(cnt AS INT64) AS cnt\n" +
                    "FROM `cannerflow-286003`.cml_temp." + tableName);
        }
        finally {
            bigQueryClient.dropTable(tableId);
        }
    }

    @Test
    public void testBigQueryGroupByOrdinal()
    {
        assertThat(bigQuerySqlConverter.convert(
                "SELECT o_custkey, COUNT(*) AS cnt\n" +
                        "FROM \"cannerflow-286003\".\"tpch_tiny\".\"orders\"\n" +
                        "GROUP BY 1", SessionContext.builder().build()))
                .isEqualTo("SELECT o_custkey, COUNT(*) AS cnt\n" +
                        "FROM `cannerflow-286003`.tpch_tiny.orders\n" +
                        "GROUP BY o_custkey");
    }

    // test case-sensitive sql in mv
    @Test
    public void testBigQueryCaseSensitiveMVReplace()
    {
        String tableName = "mv_case_sensitive" + randomTableSuffix();
        TableId tableId = TableId.of("cml_temp", tableName);
        TableDefinition tableDefinition = MaterializedViewDefinition.of("SELECT b FROM cannerflow-286003.cml_temp.CANNER WHERE b = '1'");
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        try {
            bigQueryClient.createTable(tableInfo);
            String output = bigQuerySqlConverter.convert(
                    "SELECT b FROM \"cannerflow-286003\".\"cml_temp\".\"CANNER\" WHERE b = '1'", SessionContext.builder().build());
            assertThat(output).isEqualTo("SELECT *\n" +
                    "FROM `cannerflow-286003`.cml_temp." + tableName);
        }
        finally {
            bigQueryClient.dropTable(tableId);
        }
    }

    @Test
    public void testCaseSensitive()
    {
        assertThat(bigQuerySqlConverter.convert("SELECT a FROM \"cannerflow-286003\".\"cml_temp\".\"canner\"", SessionContext.builder().build()))
                .isEqualTo("SELECT a\n" +
                        "FROM `cannerflow-286003`.cml_temp.canner");
        assertThat(bigQuerySqlConverter.convert("SELECT b FROM \"cannerflow-286003\".\"cml_temp\".\"CANNER\"", SessionContext.builder().build()))
                .isEqualTo("SELECT b\n" +
                        "FROM `cannerflow-286003`.cml_temp.CANNER");
    }
}
