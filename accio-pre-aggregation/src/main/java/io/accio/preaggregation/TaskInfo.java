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

package io.accio.preaggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.accio.preaggregation.dto.PreAggregationTable;

import java.time.Instant;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskInfo
{
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

    private final String taskId;
    private final String catalogName;
    private final String schemaName;
    private List<PreAggregationTable> preAggregationTables;

    private TaskStatus taskStatus;
    private final Instant startTime;
    private Instant endTime;

    public TaskInfo(String taskId, String catalogName, String schemaName, TaskStatus taskStatus, Instant startTime)
    {
        this(taskId, catalogName, schemaName, taskStatus, emptyList(), startTime, null);
    }

    @JsonCreator
    public TaskInfo(
            @JsonProperty("taskId") String taskId,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("taskStatus") TaskStatus taskStatus,
            @JsonProperty("preAggregationTables") List<PreAggregationTable> preAggregationTables,
            @JsonProperty("startTime") Instant startTime,
            @JsonProperty("endTime") Instant endTime)

    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.taskStatus = requireNonNull(taskStatus, "taskStatus is null");
        this.preAggregationTables = preAggregationTables == null ? emptyList() : preAggregationTables;
        this.startTime = requireNonNull(startTime, "startTime is null");
        this.endTime = endTime;
    }

    public boolean inProgress()
    {
        return taskStatus.inProgress();
    }

    @JsonProperty
    public String getTaskId()
    {
        return taskId;
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
        return catalogName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public List<PreAggregationTable> getPreAggregationTables()
    {
        return preAggregationTables;
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

    public TaskInfo setPreAggregationTables(List<PreAggregationTable> preAggregationTables)
    {
        this.preAggregationTables = preAggregationTables;
        return this;
    }
}
