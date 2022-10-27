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

package io.cml.connector.canner.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableDto
{
    private String id;
    private String displayName;
    private String sqlName;
    private String dataSourceId;
    private String workspaceId;
    private Map<String, String> properties;
    private List<Map<String, Object>> columns;
    private List<Map<String, Object>> unsupportedColumns;
    private Instant createDate;
    private Instant updateDate;
    private Map<String, Object> dataSource;
    private String status;
    private Boolean semantic;

    public TableDto() {}

    @JsonCreator
    public TableDto(
            @JsonProperty("id") String id,
            @JsonProperty("createDate") Instant createDate,
            @JsonProperty("updateDate") Instant updateDate,
            @JsonProperty("displayName") String displayName,
            @JsonProperty("sqlName") String sqlName,
            @JsonProperty("dataSourceId") String dataSourceId,
            @JsonProperty("workspaceId") String workspaceId,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("columns") List<Map<String, Object>> columns,
            @JsonProperty("unsupportedColumns") List<Map<String, Object>> unsupportedColumns,
            @JsonProperty("dataSource") Map<String, Object> dataSource,
            @JsonProperty("status") String status,
            @JsonProperty("semantic") Boolean semantic)
    {
        this.id = id;
        this.createDate = createDate;
        this.updateDate = updateDate;
        this.displayName = displayName;
        this.sqlName = sqlName;
        this.dataSourceId = dataSourceId;
        this.workspaceId = workspaceId;
        this.properties = properties;
        this.columns = columns;
        this.unsupportedColumns = unsupportedColumns;
        this.dataSource = dataSource;
        this.status = status;
        this.semantic = semantic;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    @JsonProperty
    public String getDataSourceId()
    {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId)
    {
        this.dataSourceId = dataSourceId;
    }

    @JsonProperty
    public String getDisplayName()
    {
        return displayName;
    }

    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
    }

    @JsonProperty
    public String getSqlName()
    {
        return sqlName;
    }

    public void setSqlName(String sqlName)
    {
        this.sqlName = sqlName;
    }

    @JsonProperty
    public List<Map<String, Object>> getColumns()
    {
        return columns;
    }

    public void setColumns(List<Map<String, Object>> columns)
    {
        this.columns = columns;
    }

    @JsonProperty
    public List<Map<String, Object>> getUnsupportedColumns()
    {
        return unsupportedColumns;
    }

    public void setUnsupportedColumns(List<Map<String, Object>> unsupportedColumns)
    {
        this.unsupportedColumns = unsupportedColumns;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
    }

    public void setProperties(Map<String, String> properties)
    {
        this.properties = properties;
    }

    @JsonProperty
    public Instant getCreateDate()
    {
        return createDate;
    }

    public void setCreateDate(Instant createDate)
    {
        this.createDate = createDate;
    }

    @JsonProperty
    public Instant getUpdateDate()
    {
        return updateDate;
    }

    public void setUpdateDate(Instant updateDate)
    {
        this.updateDate = updateDate;
    }

    @JsonProperty
    public String getWorkspaceId()
    {
        return workspaceId;
    }

    public void setWorkspaceId(String workspaceId)
    {
        this.workspaceId = workspaceId;
    }

    @JsonProperty
    public Map<String, Object> getDataSource()
    {
        return dataSource;
    }

    public void setDataSource(Map<String, Object> dataSource)
    {
        this.dataSource = dataSource;
    }

    @JsonProperty
    public String getStatus()
    {
        return status;
    }

    public void setStatus(String status)
    {
        this.status = status;
    }

    @JsonProperty
    public Boolean isSemantic()
    {
        return semantic;
    }

    public void setSemantic(Boolean semantic)
    {
        this.semantic = semantic;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("displayName", displayName)
                .add("dataSourceId", dataSourceId)
                .add("workspaceId", workspaceId)
                .add("sqlName", sqlName)
                .add("createDate", createDate)
                .add("updateDate", updateDate)
                .add("properties", properties)
                .add("columns", columns)
                .add("unsupportedColumns", unsupportedColumns)
                .add("dataSource", dataSource)
                .add("status", status)
                .add("semantic", semantic)
                .toString();
    }
}
