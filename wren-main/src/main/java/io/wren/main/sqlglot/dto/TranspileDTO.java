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

package io.wren.main.sqlglot.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TranspileDTO
{
    private String sql;
    private String read;
    private String write;

    @JsonCreator
    public TranspileDTO(
            @JsonProperty("sql") String sql,
            @JsonProperty("read") String read,
            @JsonProperty("write") String write)
    {
        this.sql = sql;
        this.read = read;
        this.write = write;
    }

    @JsonProperty
    public String getSql()
    {
        return sql;
    }

    public void setSql(String sql)
    {
        this.sql = sql;
    }

    @JsonProperty
    public String getRead()
    {
        return read;
    }

    public void setRead(String read)
    {
        this.read = read;
    }

    @JsonProperty
    public String getWrite()
    {
        return write;
    }

    public void setWrite(String write)
    {
        this.write = write;
    }
}
