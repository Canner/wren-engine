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

package io.cml.graphml;

import io.cml.graphml.dto.Column;
import io.cml.graphml.dto.EnumDefinition;
import io.cml.graphml.dto.Model;
import io.cml.graphml.dto.Relationship;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static io.cml.graphml.StandardType.STRING;
import static io.cml.graphml.dto.JoinType.MANY_TO_MANY;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGraphML
{
    private GraphML graphML;

    @BeforeClass
    public void init()
            throws IOException, URISyntaxException
    {
        graphML = GraphML.fromJson(Files.readString(Path.of(
                requireNonNull(getClass().getClassLoader().getResource("Manifest.json")).toURI())));
    }

    @Test
    public void testListAndGetModels()
    {
        assertThat(graphML.listModels().size()).isEqualTo(2);

        Optional<Model> user = graphML.getModel("User");
        assertThat(user).isPresent();
        assertThat(user.get().getRefSql()).isEqualTo("SELECT * FROM User");
        assertThat(user.get().getColumns().stream().map(Column::getName).collect(toUnmodifiableList()))
                .isEqualTo(List.of("id", "email"));
        assertThat(user.get().getColumns().stream().map(Column::getType).collect(toUnmodifiableList()))
                .isEqualTo(List.of(STRING.name(), STRING.name()));
        assertThat(user.get().getColumns().stream().map(col -> col.getRelationship().isPresent()).collect(toUnmodifiableList()))
                .isEqualTo(List.of(false, false));

        assertThat(graphML.getModel("Book")).isPresent();
        assertThat(graphML.getModel("notfound")).isNotPresent();
    }

    @Test
    public void testListAndGetRelationships()
    {
        assertThat(graphML.listRelationships().size()).isEqualTo(1);

        Optional<Relationship> bookUser = graphML.getRelationship("BookUser");
        assertThat(bookUser).isPresent();
        assertThat(bookUser.get().getModels()).isEqualTo(List.of("Book", "User"));
        assertThat(bookUser.get().getJoinType()).isEqualTo(MANY_TO_MANY);
        assertThat(bookUser.get().getCondition()).isEqualTo("user.id = book.authorId");
        assertThat(graphML.getRelationship("notfound")).isNotPresent();
    }

    @Test
    public void testListAndGetEnums()
    {
        assertThat(graphML.listEnums().size()).isEqualTo(1);
        Optional<EnumDefinition> enumField = graphML.getEnum("STATUS");
        assertThat(enumField).isPresent();
        assertThat(enumField.get().getValues()).isEqualTo(List.of("OK", "NOT_OK"));
        assertThat(graphML.getEnum("notfound")).isNotPresent();
    }
}
