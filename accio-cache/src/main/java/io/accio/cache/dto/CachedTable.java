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
package io.accio.cache.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CachedTable
{
    private final String name;
    private final Optional<String> errorMessage;
    private final Duration refreshTime;
    private final Instant createDate;

    @JsonCreator
    public CachedTable(
            @JsonProperty("name") String name,
            @JsonProperty("errorMessage") Optional<String> errorMessage,
            @JsonProperty("refreshTime") Duration refreshTime,
            @JsonProperty("createDate") Instant createDate)
    {
        this.name = requireNonNull(name, "name is null");
        this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
        this.refreshTime = requireNonNull(refreshTime, "refreshTime is null");
        this.createDate = requireNonNull(createDate, "createDate is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getErrorMessage()
    {
        return errorMessage;
    }

    @JsonProperty
    public Duration getRefreshTime()
    {
        return refreshTime;
    }

    @JsonProperty
    public Instant getCreateDate()
    {
        return createDate;
    }
}
