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

package io.graphmdl.graphml.base.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static io.graphmdl.graphml.base.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

public class Metric
{
    private final String name;
    private final String baseModel;
    private final List<Column> dimension;
    private final List<Column> measure;

    public static Metric metric(String name, String baseModel, List<Column> dimension, List<Column> measure)
    {
        return new Metric(name, baseModel, dimension, measure);
    }

    @JsonCreator
    public Metric(
            @JsonProperty("name") String name,
            @JsonProperty("baseModel") String baseModel,
            @JsonProperty("dimension") List<Column> dimension,
            @JsonProperty("measure") List<Column> measure)
    {
        this.name = requireNonNull(name, "name is null");
        this.baseModel = requireNonNull(baseModel, "baseModel is null");
        this.dimension = requireNonNull(dimension, "dimension is null");
        this.measure = requireNonNull(measure, "measure is null");
        checkArgument(measure.size() > 0, "the number of measures should be one at least");
    }

    public String getName()
    {
        return name;
    }

    public String getBaseModel()
    {
        return baseModel;
    }

    public List<Column> getDimension()
    {
        return dimension;
    }

    public List<Column> getMeasure()
    {
        return measure;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Metric that = (Metric) obj;
        return Objects.equals(name, that.name)
                && Objects.equals(baseModel, that.baseModel)
                && Objects.equals(dimension, that.dimension)
                && Objects.equals(measure, that.measure);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, baseModel, dimension, measure);
    }
}
