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

import com.google.common.collect.ImmutableMap;
import io.wren.base.AnalyzedMDL;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.WrenMDL;
import io.wren.base.WrenTypes;
import io.wren.base.dto.Column;
import io.wren.base.dto.Model;
import io.wren.cache.TaskInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.wren.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.wren.cache.TaskInfo.TaskStatus.DONE;
import static io.wren.cache.TaskInfo.TaskStatus.QUEUED;
import static io.wren.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDuckdbRetryCacheTask
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

    @AfterMethod
    public void cleanUp()
    {
        cacheManager.get().removeCacheIfExist(ordersName);
    }

    @Override
    protected ImmutableMap.Builder<String, String> getProperties()
    {
        return ImmutableMap.<String, String>builder()
                .putAll(super.getProperties().build())
                .put("duckdb.max-cache-table-size-ratio", "0")
                .put("pg-wire-protocol.enabled", "true");
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
        Optional<TaskInfo> taskInfo = cacheManager.get().getTaskInfo(ordersName).join();
        assertThat(taskInfo).isPresent();
        assertThat(taskInfo.get().getTaskStatus()).isEqualTo(DONE);
        assertThat(cacheManager.get().cacheScheduledFutureExists(ordersName)).isFalse();
        assertThat(cacheManager.get().retryScheduledFutureExists(ordersName)).isTrue();
    }
}
