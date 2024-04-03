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
package io.wren.testing.bigquery;

import com.google.inject.Key;
import io.wren.base.config.BigQueryConfig;
import io.wren.base.config.ConfigManager;
import io.wren.cache.CacheService;
import io.wren.cache.PathInfo;
import io.wren.main.connector.CacheServiceManager;
import io.wren.main.metadata.Metadata;
import io.wren.testing.TestingWrenServer;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.wren.main.connector.bigquery.BigQueryCacheService.getTableLocationPrefix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryCacheService
        extends AbstractCacheTest
{
    private CacheServiceManager cacheServiceManager;
    private BigQueryConfig bigQueryConfig;
    private Metadata metadata;
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

    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        TestingWrenServer wrenServer = super.createWrenServer();
        cacheServiceManager = (CacheServiceManager) wrenServer.getInstance(Key.get(CacheService.class));
        bigQueryConfig = wrenServer.getInstance(Key.get(ConfigManager.class)).getConfig(BigQueryConfig.class);
        metadata = wrenServer.getInstance(Key.get(Metadata.class));
        return wrenServer;
    }

    @Test
    public void testBigQueryCacheService()
    {
        Optional<PathInfo> pathInfo = cacheServiceManager.createCache(
                bigQueryConfig.getProjectId().orElseThrow(AssertionError::new),
                "tpch_tiny",
                "Revenue",
                query);
        assertThat(pathInfo).isPresent();
        String bucketName = bigQueryConfig.getBucketName().orElseThrow(AssertionError::new);
        String prefix = getTableLocationPrefix(pathInfo.get().getPath()).orElseThrow(AssertionError::new);
        assertThat(metadata.getCacheStorageClient().checkFolderExists(bucketName, prefix)).isTrue();
        cacheServiceManager.deleteTarget(pathInfo.get());
        assertThat(metadata.getCacheStorageClient().checkFolderExists(bucketName, prefix)).isFalse();
    }

    @Test
    public void testDeleteNonExistentDirectory()
    {
        // No exception should be thrown when deleting a non-existent directory
        PathInfo pathInfo = PathInfo.of("aa/bb/cc", "*.parquet");
        cacheServiceManager.deleteTarget(pathInfo);
    }
}
