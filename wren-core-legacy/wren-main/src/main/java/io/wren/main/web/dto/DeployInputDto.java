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

package io.wren.main.web.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.wren.base.dto.Manifest;

public class DeployInputDto
{
    private final Manifest manifest;
    private final String version;

    @JsonCreator
    public DeployInputDto(
            @JsonProperty("manifest") Manifest manifest,
            @JsonProperty("version") String version)
    {
        this.manifest = manifest;
        this.version = version;
    }

    @JsonProperty
    public Manifest getManifest()
    {
        return manifest;
    }

    @JsonProperty
    public String getVersion()
    {
        return version;
    }
}
