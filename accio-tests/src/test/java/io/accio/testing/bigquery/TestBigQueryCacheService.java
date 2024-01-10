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
package io.accio.testing.bigquery;

import com.google.api.gax.rpc.HeaderProvider;
import io.accio.cache.PathInfo;
import io.accio.connector.bigquery.BigQueryClient;
import io.accio.connector.bigquery.GcsStorageClient;
import io.accio.main.connector.bigquery.BigQueryCacheService;
import io.accio.main.connector.bigquery.BigQueryConfig;
import io.accio.main.connector.bigquery.BigQueryCredentialsSupplier;
import io.accio.main.connector.bigquery.BigQueryMetadata;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.accio.main.connector.bigquery.BigQueryCacheService.getTableLocationPrefix;
import static io.accio.server.module.BigQueryConnectorModule.createHeaderProvider;
import static io.accio.server.module.BigQueryConnectorModule.provideBigQuery;
import static io.accio.server.module.BigQueryConnectorModule.provideBigQueryCredentialsSupplier;
import static io.accio.server.module.BigQueryConnectorModule.provideGcsStorageClient;
import static java.lang.System.getenv;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryCacheService
{
    private final BigQueryCacheService bigQueryCacheService;
    private final BigQueryConfig bigQueryConfig;
    private final GcsStorageClient gcsStorageClient;
    private final String query = "WITH\n" +
            "  Orders AS (\n" +
            "   SELECT\n" +
            "     o_orderkey orderkey\n" +
            "   , o_custkey custkey\n" +
            "   , o_orderstatus orderstatus\n" +
            "   , o_totalprice totalprice\n" +
            "   , o_orderdate orderdate\n" +
            "   FROM\n" +
            "     (\n" +
            "      SELECT *\n" +
            "      FROM\n" +
            "        canner-cml.tpch_tiny.orders\n" +
            "   ) \n" +
            ") \n" +
            ", Revenue AS (\n" +
            "   SELECT\n" +
            "     custkey\n" +
            "   , sum(totalprice) revenue\n" +
            "   FROM\n" +
            "     Orders\n" +
            "   GROUP BY 1\n" +
            ") \n" +
            "SELECT *\n" +
            "FROM\n" +
            "  Revenue";

    private TestBigQueryCacheService()
    {
        this.bigQueryConfig = new BigQueryConfig();
        bigQueryConfig.setLocation("asia-east1")
                .setCredentialsKey(getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .setProjectId(getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .setBucketName(getenv("TEST_BIG_QUERY_BUCKET_NAME"));
        BigQueryCredentialsSupplier bigQueryCredentialsSupplier = provideBigQueryCredentialsSupplier(bigQueryConfig);
        HeaderProvider headerProvider = createHeaderProvider();
        BigQueryClient bigQueryClient = provideBigQuery(bigQueryConfig, headerProvider, bigQueryCredentialsSupplier);
        gcsStorageClient = provideGcsStorageClient(bigQueryConfig, headerProvider, bigQueryCredentialsSupplier);
        BigQueryMetadata bigQueryMetadata = new BigQueryMetadata(bigQueryClient, bigQueryConfig);
        this.bigQueryCacheService = new BigQueryCacheService(bigQueryMetadata, bigQueryConfig, gcsStorageClient);
    }

    @Test
    public void testBigQueryCacheService()
    {
        Optional<PathInfo> pathInfo = bigQueryCacheService.createCache(
                bigQueryConfig.getProjectId().orElseThrow(AssertionError::new),
                "tpch_tiny",
                "Revenue",
                query);
        assertThat(pathInfo).isPresent();
        String bucketName = bigQueryConfig.getBucketName().orElseThrow(AssertionError::new);
        String prefix = getTableLocationPrefix(pathInfo.get().getPath()).orElseThrow(AssertionError::new);
        assertThat(gcsStorageClient.checkFolderExists(bucketName, prefix)).isTrue();
        bigQueryCacheService.deleteTarget(pathInfo.get());
        assertThat(gcsStorageClient.checkFolderExists(bucketName, prefix)).isFalse();
    }

    @Test
    public void testDeleteNonExistentDirectory()
    {
        // No exception should be thrown when deleting a non-existent directory
        PathInfo pathInfo = PathInfo.of("aa/bb/cc", "*.parquet");
        bigQueryCacheService.deleteTarget(pathInfo);
    }
}
