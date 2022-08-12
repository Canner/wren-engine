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
import io.cml.metrics.Metric;
import io.cml.metrics.MetricSql;
import io.cml.server.module.BigQueryConnectorModule;
import io.cml.spi.SessionContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.cml.Utils.randomTableSuffix;
import static io.cml.metrics.Metric.Filter.Operator.GREATER_THAN;
import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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
                .setLocation("asia-east1");

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
                        "FROM canner-cml.tpch_tiny.orders\n" +
                        "GROUP BY o_custkey");
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        try {
            bigQueryClient.createTable(tableInfo);

            String output = bigQuerySqlConverter.convert(
                    "SELECT o_custkey, COUNT(*) as cnt\n" +
                            "FROM \"canner-cml\".\"tpch_tiny\".\"orders\"\n" +
                            "GROUP BY o_custkey", SessionContext.builder().build());
            assertThat(output).isEqualTo("SELECT o_custkey, CAST(cnt AS INT64) AS cnt\n" +
                    "FROM `canner-cml`.cml_temp." + tableName);
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
                        "FROM \"canner-cml\".\"tpch_tiny\".\"orders\"\n" +
                        "GROUP BY 1", SessionContext.builder().build()))
                .isEqualTo("SELECT o_custkey, COUNT(*) AS cnt\n" +
                        "FROM `canner-cml`.tpch_tiny.orders\n" +
                        "GROUP BY o_custkey");
    }

    // test case-sensitive sql in mv
    @Test
    public void testBigQueryCaseSensitiveMVReplace()
    {
        String tableName = "mv_case_sensitive" + randomTableSuffix();
        TableId tableId = TableId.of("cml_temp", tableName);
        TableDefinition tableDefinition = MaterializedViewDefinition.of("SELECT b FROM canner-cml.cml_temp.CANNER WHERE b = '1'");
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        try {
            bigQueryClient.createTable(tableInfo);
            String output = bigQuerySqlConverter.convert(
                    "SELECT b FROM \"canner-cml\".\"cml_temp\".\"CANNER\" WHERE b = '1'", SessionContext.builder().build());
            assertThat(output).isEqualTo("SELECT *\n" +
                    "FROM `canner-cml`.cml_temp." + tableName);
        }
        finally {
            bigQueryClient.dropTable(tableId);
        }
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

    @Test
    public void testCreateMetricTable()
    {
        Metric metric = Metric.builder()
                .setName("metric")
                .setSource("canner-cml.tpch_tiny.orders")
                .setType(Metric.Type.AVG)
                .setSql("o_totalprice")
                .setDimensions(Set.of("o_orderstatus"))
                .setTimestamp("o_orderdate")
                .setTimeGrains(Set.of(Metric.TimeGrain.MONTH))
                .setFilters(Set.of(new Metric.Filter("o_orderkey", GREATER_THAN, "1")))
                .build();

        List<MetricSql> metricSqls = MetricSql.of(metric);
        assertThat(metricSqls.size()).isEqualTo(1);
        MetricSql metricSql = metricSqls.get(0);

        assertThatCode(() -> bigQueryClient.queryDryRun(Optional.of("cml_temp"), metricSql.sql("cml_temp"), List.of()))
                .doesNotThrowAnyException();
    }
}
