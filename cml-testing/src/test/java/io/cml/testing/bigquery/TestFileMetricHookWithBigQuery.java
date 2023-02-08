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

import io.cml.connector.bigquery.BigQueryClient;
import io.cml.connector.bigquery.BigQueryConfig;
import io.cml.connector.bigquery.BigQueryMetadata;
import io.cml.connector.bigquery.BigQuerySqlConverter;
import io.cml.metadata.Metadata;
import io.cml.metrics.FileMetricStore;
import io.cml.metrics.MetricHook;
import io.cml.metrics.MetricStore;
import io.cml.server.module.BigQueryConnectorModule;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.sql.SqlConverter;
import io.cml.testing.AbstractTestMetricHook;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;

import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;

@Test(singleThreaded = true)
public class TestFileMetricHookWithBigQuery
        extends AbstractTestMetricHook
{
    private final BigQueryClient bigQueryClient;
    private final Metadata metadata;
    private final MetricStore metricStore;
    private final SqlConverter sqlConverter;
    private final MetricHook metricHook;

    public TestFileMetricHookWithBigQuery()
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

        this.metadata = new BigQueryMetadata(bigQueryClient, config);
        this.metricStore = new FileMetricStore(Path.of(requireNonNull(getenv("TEST_CML_FILE_METRIC_STORE_HOME"))));
        this.metricHook = new MetricHook(metricStore, metadata);
        this.sqlConverter = new BigQuerySqlConverter(metadata);
    }

    @Override
    protected MetricStore getMetricStore()
    {
        return metricStore;
    }

    @Override
    protected Metadata getMetadata()
    {
        return metadata;
    }

    @Override
    protected SqlConverter getSqlConverter()
    {
        return sqlConverter;
    }

    @Override
    protected MetricHook getMetricHook()
    {
        return metricHook;
    }

    @Override
    protected void dropTables(List<SchemaTableName> createdTables)
    {
        createdTables.forEach(bigQueryClient::dropTable);
    }
}
