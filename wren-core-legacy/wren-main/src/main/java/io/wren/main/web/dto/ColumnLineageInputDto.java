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
import io.wren.base.dto.Manifest;

public class ColumnLineageInputDto
{
    private final Manifest manifest;
    private final String modelName;
    private final String columnName;

    @JsonCreator
    public ColumnLineageInputDto(
            @JsonProperty("manifest") Manifest manifest,
            @JsonProperty("modelName") String modelName,
            @JsonProperty("columnName") String columnName)
    {
        this.manifest = manifest;
        this.modelName = modelName;
        this.columnName = columnName;
    }

    @JsonProperty
    public Manifest getManifest()
    {
        return manifest;
    }

    @JsonProperty
    public String getModelName()
    {
        return modelName;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }
}
