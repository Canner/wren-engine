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

package io.wren.cache;

import com.google.common.annotations.VisibleForTesting;
import io.wren.base.AnalyzedMDL;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.Parameter;
import io.wren.base.WrenException;
import io.wren.base.dto.CacheInfo;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public interface CacheManager
{
    default ConnectorRecordIterator query(String sql, List<Parameter> parameters)
    {
        throw new WrenException(GENERIC_INTERNAL_ERROR, "Enable Wren Protocol to use this feature");
    }

    default void removeCacheIfExist(String catalogName, String schemaName) {}

    default void removeCacheIfExist(CatalogSchemaTableName catalogSchemaTableName) {}

    default boolean cacheScheduledFutureExists(CatalogSchemaTableName catalogSchemaTableName)
    {
        return false;
    }

    @VisibleForTesting
    default boolean retryScheduledFutureExists(CatalogSchemaTableName catalogSchemaTableName)
    {
        return false;
    }

    default CompletableFuture<List<TaskInfo>> createTask(AnalyzedMDL analyzedMDL)
    {
        return CompletableFuture.completedFuture(List.of());
    }

    default CompletableFuture<TaskInfo> createTask(AnalyzedMDL analyzedMDL, CacheInfo cacheInfo)
    {
        throw new WrenException(GENERIC_INTERNAL_ERROR, "Enable Wren Protocol to use this feature");
    }

    default CompletableFuture<List<TaskInfo>> listTaskInfo(String catalogName, String schemaName)
    {
        throw new WrenException(GENERIC_INTERNAL_ERROR, "Enable Wren Protocol to use this feature");
    }

    default CompletableFuture<Optional<TaskInfo>> getTaskInfo(CatalogSchemaTableName catalogSchemaTableName)
    {
        throw new WrenException(GENERIC_INTERNAL_ERROR, "Enable Wren Protocol to use this feature");
    }

    @VisibleForTesting
    default void untilTaskDone(CatalogSchemaTableName name) {}

    default List<Object> getDuckDBSettings()
    {
        throw new WrenException(GENERIC_INTERNAL_ERROR, "Enable Wren Protocol to use this feature");
    }
}
