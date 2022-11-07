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

package io.cml.graphml.connector.canner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.cml.graphml.Utils.checkArgument;
import static io.cml.graphml.Utils.firstNonNull;
import static io.cml.graphml.connector.canner.FixJsonDataUtils.fixData;
import static java.util.Objects.requireNonNull;

public class TrinoQueryResult
{
    private final String id;
    private final URI infoUri;
    private final URI partialCancelUri;
    private final URI nextUri;
    private final List<Map<String, Object>> columns;
    private final Iterable<List<Object>> data;
    private final Map<String, Object> stats;
    private final Map<String, Object> error;
    private final List<Map<String, Object>> warnings;
    private final String updateType;
    private final Long updateCount;

    @JsonCreator
    public TrinoQueryResult(
            @JsonProperty("id") String id,
            @JsonProperty("infoUri") URI infoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("nextUri") URI nextUri,
            @JsonProperty("columns") List<Map<String, Object>> columns,
            @JsonProperty("data") List<List<Object>> data,
            @JsonProperty("stats") Map<String, Object> stats,
            @JsonProperty("error") Map<String, Object> error,
            @JsonProperty("warnings") List<Map<String, Object>> warnings,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateCount") Long updateCount)
    {
        this(
                id,
                infoUri,
                partialCancelUri,
                nextUri,
                columns,
                fixData(columns, data),
                stats,
                error,
                firstNonNull(warnings, List.of()),
                updateType,
                updateCount);
    }

    public TrinoQueryResult(
            String id,
            URI infoUri,
            URI partialCancelUri,
            URI nextUri,
            List<Map<String, Object>> columns,
            Iterable<List<Object>> data,
            Map<String, Object> stats,
            Map<String, Object> error,
            List<Map<String, Object>> warnings,
            String updateType,
            Long updateCount)
    {
        this.id = requireNonNull(id, "id is null");
        this.infoUri = requireNonNull(infoUri, "infoUri is null");
        this.partialCancelUri = partialCancelUri;
        this.nextUri = nextUri;
        this.columns = (columns != null) ? List.copyOf(columns) : null;
        this.data = data;
        checkArgument(data == null || columns != null, "data present without columns");
        this.stats = requireNonNull(stats, "stats is null");
        this.error = error;
        this.warnings = List.copyOf(requireNonNull(warnings, "warnings is null"));
        this.updateType = updateType;
        this.updateCount = updateCount;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public URI getInfoUri()
    {
        return infoUri;
    }

    @Nullable
    @JsonProperty
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    @Nullable
    @JsonProperty
    public URI getNextUri()
    {
        return nextUri;
    }

    @Nullable
    @JsonProperty
    public List<Map<String, Object>> getColumns()
    {
        return columns;
    }

    @Nullable
    @JsonProperty
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    @JsonProperty
    public Map<String, Object> getStats()
    {
        return stats;
    }

    @Nullable
    @JsonProperty
    public Map<String, Object> getError()
    {
        return error;
    }

    @JsonProperty
    public List<Map<String, Object>> getWarnings()
    {
        return warnings;
    }

    @Nullable
    @JsonProperty
    public String getUpdateType()
    {
        return updateType;
    }

    @Nullable
    @JsonProperty
    public Long getUpdateCount()
    {
        return updateCount;
    }

    @Override
    public String toString()
    {
        return "TrinoQueryResult{" +
                "id='" + id + '\'' +
                ", infoUri=" + infoUri +
                ", partialCancelUri=" + partialCancelUri +
                ", nextUri=" + nextUri +
                ", columns=" + columns +
                ", data=" + data +
                ", stats=" + stats +
                ", error=" + error +
                ", warnings=" + warnings +
                ", updateType='" + updateType + '\'' +
                ", updateCount=" + updateCount +
                '}';
    }
}
