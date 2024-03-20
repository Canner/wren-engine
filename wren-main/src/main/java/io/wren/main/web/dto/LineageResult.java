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
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LineageResult
{
    public static LineageResult lineageResult(String datasetName, List<Column> columns)
    {
        return new LineageResult(datasetName, columns);
    }

    public static Column columnWithType(String name, String type)
    {
        return new Column(name, ImmutableMap.of("type", type));
    }

    private final String datasetName;
    private final List<Column> columns;

    @JsonCreator
    public LineageResult(
            @JsonProperty("datasetName") String datasetName,
            @JsonProperty("columns") List<Column> columns)
    {
        this.datasetName = datasetName;
        this.columns = columns;
    }

    @JsonProperty
    public String getDatasetName()
    {
        return datasetName;
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LineageResult that = (LineageResult) o;
        return Objects.equals(datasetName, that.datasetName) && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(datasetName, columns);
    }

    @Override
    public String toString()
    {
        return "LineageResult{" +
                "datasetName='" + datasetName + '\'' +
                ", columns=" + columns +
                '}';
    }

    public static class Column
    {
        private final String name;
        private final Map<String, String> properties;

        @JsonCreator
        public Column(
                @JsonProperty("name") String name,
                @JsonProperty("properties") Map<String, String> properties)
        {
            this.name = name;
            this.properties = properties;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public Map<String, String> getProperties()
        {
            return properties;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Column column = (Column) o;
            return Objects.equals(name, column.name) && Objects.equals(properties, column.properties);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, properties);
        }

        @Override
        public String toString()
        {
            return "Column{" +
                    "name='" + name + '\'' +
                    ", properties=" + properties +
                    '}';
        }
    }
}
