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
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class WorkspaceDto
{
    private String id;
    private String displayName;
    private String sqlName;
    private Instant createDate;
    private Instant updateDate;
    private String keycloakGroupId;
    private Optional<Integer> maxQueued;
    private Optional<Integer> hardConcurrencyLimit;
    private Optional<String> softMemoryLimit;
    @Nullable
    private String parentId;
    private String subWorkspaceId;

    public WorkspaceDto() {}

    public WorkspaceDto(String displayName, String sqlName, String keycloakGroupId)
    {
        this(
                null,
                null,
                null,
                displayName,
                sqlName,
                keycloakGroupId,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                null,
                null);
    }

    @JsonCreator
    public WorkspaceDto(
            @JsonProperty("id") String id,
            @JsonProperty("createDate") Instant createDate,
            @JsonProperty("updateDate") Instant updateDate,
            @JsonProperty("displayName") String displayName,
            @JsonProperty("sqlName") String sqlName,
            @JsonProperty("keycloakGroupId") String keycloakGroupId,
            @JsonProperty("maxQueued") Optional<Integer> maxQueued,
            @JsonProperty("hardConcurrencyLimit") Optional<Integer> hardConcurrencyLimit,
            @JsonProperty("softMemoryLimit") Optional<String> softMemoryLimit,
            @Nullable @JsonProperty("parentId") String parentId,
            @JsonProperty("subWorkspaceId") String subWorkspaceId)
    {
        this.id = id;
        this.createDate = createDate;
        this.updateDate = updateDate;
        this.displayName = displayName;
        this.sqlName = sqlName;
        this.keycloakGroupId = keycloakGroupId;
        this.maxQueued = maxQueued;
        this.hardConcurrencyLimit = hardConcurrencyLimit;
        this.softMemoryLimit = softMemoryLimit;
        this.parentId = parentId;
        this.subWorkspaceId = subWorkspaceId;
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
    public String getKeycloakGroupId()
    {
        return keycloakGroupId;
    }

    public void setKeycloakGroupId(String keycloakGroupId)
    {
        this.keycloakGroupId = keycloakGroupId;
    }

    @JsonProperty
    public Optional<Integer> getMaxQueued()
    {
        return maxQueued;
    }

    public void setMaxQueued(Optional<Integer> maxQueued)
    {
        this.maxQueued = maxQueued;
    }

    @JsonProperty
    public Optional<Integer> getHardConcurrencyLimit()
    {
        return hardConcurrencyLimit;
    }

    public void setHardConcurrencyLimit(Optional<Integer> hardConcurrencyLimit)
    {
        this.hardConcurrencyLimit = hardConcurrencyLimit;
    }

    @JsonProperty
    public Optional<String> getSoftMemoryLimit()
    {
        return softMemoryLimit;
    }

    public void setSoftMemoryLimit(Optional<String> softMemoryLimit)
    {
        this.softMemoryLimit = softMemoryLimit;
    }

    @Nullable
    @JsonProperty
    public String getParentId()
    {
        return parentId;
    }

    public void setParentId(@Nullable String parentId)
    {
        this.parentId = parentId;
    }

    @JsonProperty
    public String getSubWorkspaceId()
    {
        return subWorkspaceId;
    }

    public void setSubWorkspaceId(String subWorkspaceId)
    {
        this.subWorkspaceId = subWorkspaceId;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("displayName", displayName)
                .add("sqlName", sqlName)
                .add("createDate", createDate)
                .add("updateDate", updateDate)
                .add("keycloakGroupId", keycloakGroupId)
                .add("maxQueued", maxQueued)
                .add("hardConcurrencyLimit", hardConcurrencyLimit)
                .add("softMemoryLimit", softMemoryLimit)
                .add("parentId", parentId)
                .add("subWorkspaceId", subWorkspaceId)
                .toString();
    }
}
