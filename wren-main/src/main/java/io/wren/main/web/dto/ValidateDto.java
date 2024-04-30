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

import java.util.Map;

public class ValidateDto
{
    private final Manifest manifest;
    private final Map<String, Object> parameters;

    @JsonCreator
    public ValidateDto(
            @JsonProperty("manifest") Manifest manifest,
            @JsonProperty("parameters") Map<String, Object> parameters)
    {
        this.manifest = manifest;
        this.parameters = parameters;
    }

    @JsonProperty
    public Manifest getManifest()
    {
        return manifest;
    }

    @JsonProperty
    public Map<String, Object> getParameters()
    {
        return parameters;
    }
}
