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

public class SqlAnalysisInputDto
{
    private final Manifest manifest;
    private final String sql;

    @JsonCreator
    public SqlAnalysisInputDto(
            @JsonProperty("manifest") Manifest manifest,
            @JsonProperty("sql") String sql)
    {
        this.manifest = manifest;
        this.sql = sql;
    }

    @JsonProperty
    public Manifest getManifest()
    {
        return manifest;
    }

    @JsonProperty
    public String getSql()
    {
        return sql;
    }
}
