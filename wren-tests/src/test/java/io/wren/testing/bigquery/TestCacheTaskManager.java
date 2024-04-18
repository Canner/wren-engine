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
import io.wren.base.AnalyzedMDL;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.client.duckdb.DuckDBConfig;
import io.wren.base.client.duckdb.DuckDBConnectorConfig;
import io.wren.base.client.duckdb.DuckdbS3StyleStorageConfig;
import io.wren.base.config.BigQueryConfig;
import io.wren.base.config.ConfigManager;
import io.wren.base.config.PostgresConfig;
import io.wren.base.config.PostgresWireProtocolConfig;
import io.wren.base.config.WrenConfig;
import io.wren.base.dto.Column;
import io.wren.base.dto.Model;
import io.wren.cache.CacheInfoPair;
import io.wren.cache.CacheTaskManager;
import io.wren.cache.TaskInfo;
import io.wren.main.connector.duckdb.DuckDBSqlConverter;
import io.wren.main.wireprotocol.PgMetastoreImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.wren.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.wren.cache.TaskInfo.TaskStatus.QUEUED;
import static io.wren.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@Test(singleThreaded = true)
public class TestCacheTaskManager
        extends AbstractCacheTest
{
    private static final WrenMDL mdl = WrenMDL.fromManifest(withDefaultCatalogSchema()
            .setModels(List.of(
                    Model.model(
                            "Orders",
                            "select * from wrenai.tpch_tiny.orders",
                            List.of(
                                    Column.column("orderkey", WrenTypes.VARCHAR, null, false, "o_orderkey"),
                                    Column.column("custkey", WrenTypes.VARCHAR, null, false, "o_custkey")),
                            true)))
            .build());
    private static final CatalogSchemaTableName ordersName = catalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), "Orders");
    private CacheTaskManager cacheTaskManager;

    @BeforeClass
    public void setup()
    {
        cacheTaskManager = getInstance(Key.get(CacheTaskManager.class));
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
        assertThatCode(cacheTaskManager::getMemoryUsageBytes).doesNotThrowAnyException();
    }

    @Test
    public void testCheckCacheMemoryLimit()
            throws IOException
    {
        assertThatCode(() -> cacheTaskManager.checkCacheMemoryLimit()).doesNotThrowAnyException();

        DuckDBConfig duckDBConfig = new DuckDBConfig();
        duckDBConfig.setMaxCacheTableSizeRatio(0);

        ConfigManager configManager = new ConfigManager(
                new WrenConfig(),
                new PostgresConfig(),
                new BigQueryConfig(),
                duckDBConfig,
                new PostgresWireProtocolConfig(),
                new DuckdbS3StyleStorageConfig(),
                new DuckDBConnectorConfig());

        try (CacheTaskManager taskManager = new CacheTaskManager(duckDBConfig, new PgMetastoreImpl(configManager, getInstance(Key.get(DuckDBSqlConverter.class))))) {
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
        WrenMDL mdlWithWrongSql = WrenMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model(
                                "WrongOrders",
                                "wrong sql",
                                List.of(
                                        Column.column("orderkey", WrenTypes.VARCHAR, null, false, "o_orderkey"),
                                        Column.column("custkey", WrenTypes.VARCHAR, null, false, "o_custkey")),
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
        assertThat(cacheTaskManager.addCacheQueryTask(() -> 1)).isEqualTo(1);

        // test timeout
        long maxQueryTimeout = getInstance(Key.get(DuckDBConfig.class)).getMaxCacheQueryTimeout();
        assertThatCode(() -> cacheTaskManager.addCacheQueryTask(() -> {
            SECONDS.sleep(maxQueryTimeout + 1);
            return 1;
        })).hasMessageContaining("Query time limit exceeded");
    }

    @Test
    public void testAddCacheQueryDDLTask()
    {
        assertThatCode(() -> cacheTaskManager.addCacheQueryDDLTask(() -> sleepSeconds(1))).doesNotThrowAnyException();

        // test timeout
        long maxQueryTimeout = getInstance(Key.get(DuckDBConfig.class)).getMaxCacheQueryTimeout();
        assertThatCode(() -> cacheTaskManager.addCacheQueryDDLTask(() -> sleepSeconds(maxQueryTimeout + 1)))
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
