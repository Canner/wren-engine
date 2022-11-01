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

package io.cml.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Optional;

public class GraphMLStore
{
    private final GraphML graphML;

    public GraphMLStore(@Language("json") String graphMLJson)
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        graphML = objectMapper.readValue(graphMLJson, GraphML.class);
    }

    public List<GraphML.Model> listModels() {
        return graphML.models;
    }

    public Optional<GraphML.Model> getModel(String name)
    {
        return graphML.models.stream().filter(model -> model.name.equals(name)).findAny();
    }

//    public String getModelSql(String name) {
//        return "SELECT col_1 from bar";
//    }
}
