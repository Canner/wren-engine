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

import io.accio.base.CatalogSchemaTableName;
import io.accio.base.dto.Manifest;
import io.accio.cache.CacheInfoPair;
import io.accio.cache.TaskInfo;
import io.accio.cache.dto.CachedTable;
import io.accio.main.web.dto.DeployInputDto;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.accio.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.accio.base.metadata.StandardErrorCode.NOT_FOUND;
import static io.accio.cache.TaskInfo.TaskStatus.DONE;
import static io.accio.testing.WebApplicationExceptionAssert.assertWebApplicationException;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReloadCache
        extends AbstractCacheTestWithBigQuery
{
    private Path accioMDLFilePath;

    @Override
    protected Optional<String> getAccioMDLPath()
    {
        try {
            accioMDLFilePath = Files.createTempFile("acciomdl", ".json");
            rewriteFile("cache/cache_reload_1_mdl.json");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Optional.of(accioMDLFilePath.toString());
    }

    @Test
    public void testReloadCache()
            throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        String beforeName = "Revenue";
        CatalogSchemaTableName beforeCatalogSchemaTableName = new CatalogSchemaTableName("canner-cml", "tpch_tiny", beforeName);
        Optional<CacheInfoPair> cacheInfoPairOptional = getDefaultCacheInfoPair(beforeName);
        assertThat(cacheInfoPairOptional).isPresent();
        String beforeMappingName = cacheInfoPairOptional.get().getRequiredTableName();
        assertCache(beforeName);

        deployMDL("cache/cache_reload_2_mdl.json");
        waitUntilReady();
        waitUntilFinished(catalogSchemaTableName("canner-cml", "tpch_tiny", "Revenue_After"));
        assertCache("Revenue_After");

        List<Object[]> tables = queryDuckdb("show tables");
        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).doesNotContain(beforeMappingName);
        assertThat(cacheManager.get().cacheScheduledFutureExists(beforeCatalogSchemaTableName)).isFalse();
        assertThat(getDefaultCacheInfoPair(beforeMappingName)).isEmpty();

        deployMDL("cache/cache_reload_1_mdl.json");
        waitUntilReady();
        waitUntilFinished(catalogSchemaTableName("canner-cml", "tpch_tiny", "Revenue"));

        List<TaskInfo> taskInfos = getTaskInfo("canner-cml", "tpch_tiny");
        assertThat(taskInfos.size()).isEqualTo(1);
        TaskInfo taskInfo = taskInfos.get(0);
        assertCache("Revenue");
        assertThat(taskInfo.getCatalogName()).isEqualTo("canner-cml");
        assertThat(taskInfo.getSchemaName()).isEqualTo("tpch_tiny");
        assertThat(taskInfo.getTaskStatus()).isEqualTo(DONE);
        assertThat(taskInfo.getStartTime()).isNotNull();
        assertThat(taskInfo.getEndTime()).isNotNull();
        assertThat(taskInfo.getEndTime()).isAfter(taskInfo.getStartTime());

        CachedTable cachedTable = taskInfo.getCachedTable();
        assertThat(cachedTable.getErrorMessage()).isEmpty();
        assertThat(cachedTable.getName()).isEqualTo("Revenue");
        assertThat(cachedTable.getRefreshTime()).isEqualTo(Duration.valueOf("5m"));
        assertThat(cachedTable.getCreateDate()).isNotNull();

        deployMDL("cache/cache_reload_3_mdl.json");
        waitUntilReady();
        taskInfo = waitUntilFinished(catalogSchemaTableName("canner-cml", "tpch_tiny", "Revenue_Fake"));
        cachedTable = taskInfo.getCachedTable();
        assertThat(cachedTable.getErrorMessage()).isPresent();
        assertThat(taskInfo.getEndTime()).isAfter(taskInfo.getStartTime());

        assertWebApplicationException(() -> getTaskInfo(catalogSchemaTableName("fake", "fake", "fake")))
                .hasHTTPStatus(404)
                .hasErrorCode(NOT_FOUND)
                .hasErrorMessageMatches("Task .* not found.");
    }

    private void deployMDL(String resourcePath)
            throws IOException
    {
        deployMDL(new DeployInputDto(Manifest.MANIFEST_JSON_CODEC.fromJson(Files.readString(Path.of(getClass().getClassLoader().getResource(resourcePath).getPath()))), null));
    }

    private void assertCache(String name)
    {
        CatalogSchemaTableName mapping = new CatalogSchemaTableName("canner-cml", "tpch_tiny", name);
        Optional<CacheInfoPair> cacheInfoPairOptional = getDefaultCacheInfoPair(name);
        assertThat(cacheInfoPairOptional).isPresent();
        String mappingName = cacheInfoPairOptional.get().getRequiredTableName();
        List<Object[]> tables = queryDuckdb("show tables");
        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).contains(mappingName);
        assertThat(cacheManager.get().cacheScheduledFutureExists(mapping)).isTrue();
    }

    private void rewriteFile(String resourcePath)
            throws IOException
    {
        Files.copy(Path.of(requireNonNull(getClass().getClassLoader().getResource(resourcePath)).getPath()), accioMDLFilePath, REPLACE_EXISTING);
    }

    private TaskInfo waitUntilFinished(CatalogSchemaTableName name)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        return supplyAsync(() -> {
            TaskInfo taskInfo;
            do {
                taskInfo = getTaskInfo(name);
                try {
                    SECONDS.sleep(1L);
                }
                catch (InterruptedException ignored) {
                }
            }
            while (taskInfo.inProgress());
            return taskInfo;
        }, Executors.newSingleThreadExecutor()).get(10, SECONDS);
    }
}
