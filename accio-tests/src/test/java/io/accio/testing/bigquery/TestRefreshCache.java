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
import io.accio.base.AnalyzedMDL;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.dto.CacheInfo;
import io.accio.cache.TaskInfo;
import io.accio.main.AccioMetastore;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.accio.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.accio.cache.TaskInfo.TaskStatus.QUEUED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestRefreshCache
        extends AbstractCacheTest
{
    private final Supplier<AccioMDL> accioMDL = () -> getInstance(Key.get(AccioMetastore.class)).getAnalyzedMDL().getAccioMDL();

    @Override
    protected Optional<String> getAccioMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("cache/cache_frequently_mdl.json")).getPath());
    }

    @Test
    public void testRefreshFrequently()
            throws Exception
    {
        AccioMDL mdl = accioMDL.get();
        CatalogSchemaTableName revenueName = catalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), "RefreshFrequently");
        TaskInfo original = cacheManager.get().listTaskInfo(mdl.getCatalog(), mdl.getSchema()).join().stream()
                .filter(taskInfo -> taskInfo.getCatalogSchemaTableName().equals(revenueName))
                .findAny().orElseThrow(AssertionError::new);
        // make sure the end time will be changed.
        TaskInfo refreshed = getTaskInfoUntilDifferent(original);
        assertThat(original.getEndTime()).isBefore(refreshed.getEndTime());
    }

    private TaskInfo getTaskInfoUntilDifferent(TaskInfo taskInfo)
    {
        TaskInfo refreshed = null;
        long start = System.currentTimeMillis();
        long timeout = 10000;
        while (refreshed == null ||
                refreshed.getEndTime() == null ||
                refreshed.getEndTime() == taskInfo.getEndTime()) {
            refreshed = cacheManager.get().listTaskInfo(taskInfo.getCatalogName(), taskInfo.getSchemaName()).join().stream()
                    .filter(t -> t.getTableName().equals(taskInfo.getTableName()))
                    .findAny().orElseThrow(AssertionError::new);
            try {
                MILLISECONDS.sleep(100);
            }
            catch (InterruptedException ignored) {
            }

            if (System.currentTimeMillis() - start > timeout) {
                throw new RuntimeException("Wail until task info different timeout");
            }
            start = System.currentTimeMillis();
        }
        return refreshed;
    }

    // todo need a proper test
    @Test(enabled = false)
    public void testRefreshCache()
            throws InterruptedException
    {
        String before = getDefaultCacheInfoPair("RefreshFrequently").getRequiredTableName();
        // considering the refresh connects to BigQuery service, it will take some time
        Thread.sleep(3000);
        String after = getDefaultCacheInfoPair("RefreshFrequently").getRequiredTableName();
        assertThat(before).isNotEqualTo(after);
    }

    @Test
    public void testRefreshSingleModel()
    {
        AccioMDL mdl = accioMDL.get();
        List<TaskInfo> taskInfoList = cacheManager.get().listTaskInfo(mdl.getCatalog(), mdl.getSchema()).join();
        assertThat(taskInfoList.size()).isEqualTo(3);

        CatalogSchemaTableName ordersName = catalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), "Orders");
        CacheInfo orders = mdl.getCacheInfo(ordersName)
                .orElseThrow(() -> new RuntimeException("Orders not found"));

        TaskInfo original = taskInfoList.stream()
                .filter(taskInfo -> taskInfo.getCatalogSchemaTableName().equals(ordersName))
                .findAny().orElseThrow(AssertionError::new);

        TaskInfo start = cacheManager.get().createTask(new AnalyzedMDL(mdl, null), orders).join();
        assertThat(start.getTaskStatus()).isEqualTo(QUEUED);
        assertThat(start.getEndTime()).isNull();
        cacheManager.get().untilTaskDone(ordersName);

        List<TaskInfo> finished = cacheManager.get().listTaskInfo(mdl.getCatalog(), mdl.getSchema()).join();
        TaskInfo end = finished.stream()
                .filter(taskInfo -> taskInfo.getCatalogSchemaTableName().equals(ordersName))
                .findAny().orElseThrow(AssertionError::new);
        assertThat(end.getTaskStatus()).isEqualTo(TaskInfo.TaskStatus.DONE);
        assertThat(end.getEndTime()).isAfter(original.getEndTime());

        CatalogSchemaTableName customerName = catalogSchemaTableName(mdl.getCatalog(), mdl.getSchema(), "Customer");
        TaskInfo originalCustomer = taskInfoList.stream()
                .filter(taskInfo -> taskInfo.getCatalogSchemaTableName().equals(customerName))
                .findAny().orElseThrow(AssertionError::new);

        // only refresh orders, others should not be affected
        TaskInfo endCustomer = finished.stream()
                .filter(taskInfo -> taskInfo.getCatalogSchemaTableName().equals(customerName))
                .findAny().orElseThrow(AssertionError::new);
        assertThat(originalCustomer.getEndTime()).isEqualTo(endCustomer.getEndTime());
    }
}
