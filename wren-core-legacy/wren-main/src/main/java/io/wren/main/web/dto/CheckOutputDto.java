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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.wren.main.web.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CheckOutputDto
{
    public static CheckOutputDto ready(String version)
    {
        return new CheckOutputDto(Status.READY, version);
    }

    public static CheckOutputDto prepare(String version)
    {
        return new CheckOutputDto(Status.PREPARING, version);
    }

    public enum Status
    {
        READY,
        PREPARING
    }

    private final Status status;
    private final String version;

    @JsonCreator
    public CheckOutputDto(
            @JsonProperty("systemStatus") Status status,
            @JsonProperty("version") String version)
    {
        this.status = status;
        this.version = version;
    }

    @JsonProperty
    public Status getStatus()
    {
        return status;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }
}
