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

package io.cml.graphml.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Model
{
    private final String name;
    private final String refSql;
    private final List<Column> columns;

    @JsonCreator
    public Model(
            @JsonProperty("name") String name,
            @JsonProperty("refSql") String refSql,
            @JsonProperty("columns") List<Column> columns)
    {
        this.name = requireNonNull(name, "name is null");
        this.refSql = requireNonNull(refSql, "refSql is null");
        this.columns = columns == null ? List.of() : columns;
    }

    public String getName()
    {
        return name;
    }

    public String getRefSql()
    {
        return refSql;
    }

    public List<Column> getColumns()
    {
        return columns;
    }
}
