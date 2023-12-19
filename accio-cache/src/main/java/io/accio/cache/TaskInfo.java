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

package io.accio.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.metadata.SchemaTableName;
import io.accio.cache.dto.CachedTable;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskInfo
{
    public static TaskInfo copyFrom(TaskInfo taskInfo)
    {
        return new TaskInfo(taskInfo.getCatalogName(),
                taskInfo.getSchemaName(),
                taskInfo.getTableName(),
                taskInfo.getTaskStatus(),
                taskInfo.getCachedTable(),
                taskInfo.getStartTime(),
                taskInfo.getEndTime());
    }

    public enum TaskStatus
    {
        RUNNING(false),
        DONE(true);

        private final boolean doneState;

        TaskStatus(boolean doneState)
        {
            this.doneState = doneState;
        }

        public boolean isDone()
        {
            return doneState;
        }

        public boolean inProgress()
        {
            return !doneState;
        }
    }

    private final CatalogSchemaTableName catalogSchemaTableName;
    private CachedTable cachedTable;

    private TaskStatus taskStatus;
    private final Instant startTime;
    private Instant endTime;

    public TaskInfo(String catalogName, String schemaName, String tableName, TaskStatus taskStatus, Instant startTime)
    {
        this(catalogName, schemaName, tableName, taskStatus, null, startTime, null);
    }

    @JsonCreator
    public TaskInfo(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("taskStatus") TaskStatus taskStatus,
            @JsonProperty("cachedTable") CachedTable cachedTable,
            @JsonProperty("startTime") Instant startTime,
            @JsonProperty("endTime") Instant endTime)

    {
        this.catalogSchemaTableName = new CatalogSchemaTableName(requireNonNull(catalogName, "catalogName is null"),
                new SchemaTableName(requireNonNull(schemaName, "schemaName is null"),
                        requireNonNull(tableName, "tableName is null")));
        this.taskStatus = requireNonNull(taskStatus, "taskStatus is null");
        this.cachedTable = cachedTable;
        this.startTime = requireNonNull(startTime, "startTime is null");
        this.endTime = endTime;
    }

    public boolean inProgress()
    {
        return taskStatus.inProgress();
    }

    public synchronized void setTaskStatus(TaskStatus taskStatus)
    {
        this.taskStatus = taskStatus;
        endTime = taskStatus.isDone() ? Instant.now() : endTime;
    }

    @JsonProperty
    public TaskStatus getTaskStatus()
    {
        return taskStatus;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogSchemaTableName.getCatalogName();
    }

    @JsonProperty
    public String getSchemaName()
    {
        return catalogSchemaTableName.getSchemaTableName().getSchemaName();
    }

    @JsonProperty
    public String getTableName()
    {
        return catalogSchemaTableName.getSchemaTableName().getTableName();
    }

    public CatalogSchemaTableName getCatalogSchemaTableName()
    {
        return catalogSchemaTableName;
    }

    @JsonProperty
    public CachedTable getCachedTable()
    {
        return cachedTable;
    }

    @JsonProperty
    public Instant getStartTime()
    {
        return startTime;
    }

    @JsonProperty
    public Instant getEndTime()
    {
        return endTime;
    }

    public TaskInfo setCachedTable(CachedTable cachedTable)
    {
        this.cachedTable = cachedTable;
        return this;
    }

    @Override
    public String toString()
    {
        return "TaskInfo{" +
                "catalogSchemaTableName=" + catalogSchemaTableName +
                ", cachedTable=" + cachedTable +
                ", taskStatus=" + taskStatus +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
