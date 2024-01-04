package io.accio.testing.bigquery;

import com.google.common.collect.ImmutableMap;
import io.accio.base.AccioMDL;
import io.accio.base.AccioTypes;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.dto.Column;
import io.accio.base.dto.Model;
import io.accio.cache.TaskInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.accio.base.CatalogSchemaTableName.catalogSchemaTableName;
import static io.accio.cache.TaskInfo.TaskStatus.DONE;
import static io.accio.cache.TaskInfo.TaskStatus.QUEUED;
import static io.accio.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDuckdbRetryCacheTask
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
                .put("duckdb.max-cache-table-size-ratio", "0");
    }

    @Test
    public void testAddCacheTask()
    {
        Optional<Model> model = mdl.getModel("Orders");
        assertThat(model).isPresent();

        TaskInfo start = cacheManager.get().createTask(mdl, model.get()).join();
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
