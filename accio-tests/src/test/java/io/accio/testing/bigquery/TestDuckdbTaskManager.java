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

import com.google.inject.Key;
import io.accio.base.AccioMDL;
import io.accio.base.AccioTypes;
import io.accio.base.AnalyzedMDL;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.client.duckdb.DuckDBConfig;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.base.client.duckdb.DuckdbS3StyleStorageConfig;
import io.accio.base.dto.Column;
import io.accio.base.dto.Model;
import io.accio.cache.CacheInfoPair;
import io.accio.cache.DuckdbTaskManager;
import io.accio.cache.TaskInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.accio.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.accio.cache.TaskInfo.TaskStatus.QUEUED;
import static io.accio.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@Test(singleThreaded = true)
public class TestDuckdbTaskManager
        extends AbstractCacheTest
{
    private static final AccioMDL mdl = AccioMDL.fromManifest(withDefaultCatalogSchema()
            .setModels(List.of(
                    Model.model(
                            "Orders",
                            "select * from \"canner-cml\".tpch_tiny.orders",
                            List.of(
                                    Column.column("orderkey", AccioTypes.VARCHAR, null, false, "o_orderkey"),
                                    Column.column("custkey", AccioTypes.VARCHAR, null, false, "o_custkey")),
                            true)))
            .build());
    private static final CatalogSchemaTableName ordersName = catalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), "Orders");
    private DuckdbTaskManager duckdbTaskManager;

    @BeforeClass
    public void setup()
    {
        duckdbTaskManager = getInstance(Key.get(DuckdbTaskManager.class));
    }

    @AfterMethod
    public void cleanUp()
    {
        cacheManager.get().removeCacheIfExist(ordersName);
    }

    @Override
    protected Optional<CacheInfoPair> getDefaultCacheInfoPair(String name)
    {
        return Optional.ofNullable(cachedTableMapping.get().getCacheInfoPair(mdl.getCatalog(), mdl.getSchema(), name));
    }

    @Test
    public void testGetMemoryUsage()
    {
        assertThatCode(duckdbTaskManager::getMemoryUsageBytes).doesNotThrowAnyException();
    }

    @Test
    public void testCheckCacheMemoryLimit()
            throws IOException
    {
        assertThatCode(() -> duckdbTaskManager.checkCacheMemoryLimit()).doesNotThrowAnyException();

        DuckDBConfig duckDBConfig = new DuckDBConfig();
        duckDBConfig.setMaxCacheTableSizeRatio(0);
        try (DuckdbTaskManager taskManager = new DuckdbTaskManager(duckDBConfig, new DuckdbClient(duckDBConfig, new DuckdbS3StyleStorageConfig(), Optional.empty()))) {
            assertThatCode(taskManager::checkCacheMemoryLimit).hasMessageMatching("Cache memory limit exceeded. Usage: .* bytes, Limit: 0.0 bytes");
        }
    }

    @Test
    public void testAddCacheTask()
    {
        Optional<Model> model = mdl.getModel("Orders");
        assertThat(model).isPresent();

        TaskInfo start = cacheManager.get().createTask(new AnalyzedMDL(mdl, null), model.get()).join();
        assertThat(start.getTaskStatus()).isEqualTo(QUEUED);
        assertThat(start.getEndTime()).isNull();
        cacheManager.get().untilTaskDone(ordersName);
        assertCache("Orders");
    }

    @Test
    public void testAddCacheTaskWithException()
    {
        AccioMDL mdlWithWrongSql = AccioMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model(
                                "WrongOrders",
                                "wrong sql",
                                List.of(
                                        Column.column("orderkey", AccioTypes.VARCHAR, null, false, "o_orderkey"),
                                        Column.column("custkey", AccioTypes.VARCHAR, null, false, "o_custkey")),
                                true)))
                .build());
        Optional<Model> model = mdlWithWrongSql.getModel("WrongOrders");
        assertThat(model).isPresent();

        TaskInfo start = cacheManager.get().createTask(new AnalyzedMDL(mdlWithWrongSql, null), model.get()).join();
        assertThat(start.getTaskStatus()).isEqualTo(QUEUED);

        CatalogSchemaTableName wrongOrdersName = catalogSchemaTableName(mdlWithWrongSql.getCatalog(), mdlWithWrongSql.getSchema(), "WrongOrders");
        cacheManager.get().untilTaskDone(wrongOrdersName);
        assertThat(cacheManager.get().cacheScheduledFutureExists(wrongOrdersName)).isFalse();
    }

    @Test
    public void testAddCacheQueryTask()
    {
        assertThat(duckdbTaskManager.addCacheQueryTask(() -> 1)).isEqualTo(1);

        // test timeout
        long maxQueryTimeout = getInstance(Key.get(DuckDBConfig.class)).getMaxCacheQueryTimeout();
        assertThatCode(() -> duckdbTaskManager.addCacheQueryTask(() -> {
            SECONDS.sleep(maxQueryTimeout + 1);
            return 1;
        })).hasMessageContaining("Query time limit exceeded");
    }

    @Test
    public void testAddCacheQueryDDLTask()
    {
        assertThatCode(() -> duckdbTaskManager.addCacheQueryDDLTask(() -> sleepSeconds(1))).doesNotThrowAnyException();

        // test timeout
        long maxQueryTimeout = getInstance(Key.get(DuckDBConfig.class)).getMaxCacheQueryTimeout();
        assertThatCode(() -> duckdbTaskManager.addCacheQueryDDLTask(() -> sleepSeconds(maxQueryTimeout + 1)))
                .hasMessageContaining("Query time limit exceeded");
    }

    private void assertCache(String name)
    {
        Optional<CacheInfoPair> cacheInfoPairOptional = getDefaultCacheInfoPair(name);
        assertThat(cacheInfoPairOptional).isPresent();
        String mappingName = cacheInfoPairOptional.get().getRequiredTableName();
        List<Object[]> tables = queryDuckdb("show tables");
        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).contains(mappingName);
        assertThat(cacheManager.get().cacheScheduledFutureExists(ordersName)).isTrue();
    }

    private void sleepSeconds(long seconds)
    {
        try {
            SECONDS.sleep(seconds);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
