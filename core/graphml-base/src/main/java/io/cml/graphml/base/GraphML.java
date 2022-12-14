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

package io.cml.graphml.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cml.graphml.base.dto.EnumDefinition;
import io.cml.graphml.base.dto.Manifest;
import io.cml.graphml.base.dto.Model;
import io.cml.graphml.base.dto.Relationship;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GraphML
{
    private final Manifest manifest;

    public static GraphML fromJson(String manifest)
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        return new GraphML(objectMapper.readValue(manifest, Manifest.class));
    }

    public static GraphML fromManifest(Manifest manifest)
    {
        return new GraphML(manifest);
    }

    private GraphML(Manifest manifest)
    {
        this.manifest = requireNonNull(manifest, "graphML is null");
    }

    public List<Model> listModels()
    {
        return manifest.getModels();
    }

    public Optional<Model> getModel(String name)
    {
        return manifest.getModels().stream()
                .filter(model -> model.getName().equals(name))
                .findAny();
    }

    public List<Relationship> listRelationships()
    {
        return manifest.getRelationships();
    }

    public Optional<Relationship> getRelationship(String name)
    {
        return manifest.getRelationships().stream()
                .filter(relationship -> relationship.getName().equals(name))
                .findAny();
    }

    public List<EnumDefinition> listEnums()
    {
        return manifest.getEnumFields();
    }

    public Optional<EnumDefinition> getEnum(String name)
    {
        return manifest.getEnumFields().stream()
                .filter(enumField -> enumField.getName().equals(name))
                .findAny();
    }
}
