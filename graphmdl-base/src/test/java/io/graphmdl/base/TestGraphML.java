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

package io.graphmdl.base;

import io.graphmdl.base.dto.Column;
import io.graphmdl.base.dto.EnumDefinition;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.base.dto.Metric;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.Relationship;
import org.assertj.core.api.Assertions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static java.util.Locale.ENGLISH;
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
        assertThat(user.get().getRefSql()).isEqualTo("SELECT * FROM (VALUES (1, 'user1'), (2, 'user2'), (3, 'user3')) User(id, name))");
        assertThat(user.get().getColumns().stream().map(Column::getName).collect(toUnmodifiableList()))
                .isEqualTo(List.of("id", "name"));
        assertThat(user.get().getColumns().stream().map(Column::getType).collect(toUnmodifiableList()))
                .isEqualTo(List.of(GraphMLTypes.INTEGER.toUpperCase(ENGLISH), GraphMLTypes.VARCHAR.toUpperCase(ENGLISH)));
        assertThat(user.get().getColumns().stream().map(col -> col.getRelationship().isPresent()).collect(toUnmodifiableList()))
                .isEqualTo(List.of(false, false));

        Assertions.assertThat(graphML.getModel("Book")).isPresent();
        Assertions.assertThat(graphML.getModel("notfound")).isNotPresent();
    }

    @Test
    public void testListAndGetRelationships()
    {
        assertThat(graphML.listRelationships().size()).isEqualTo(1);

        Optional<Relationship> bookUser = graphML.getRelationship("BookUser");
        assertThat(bookUser).isPresent();
        assertThat(bookUser.get().getModels()).isEqualTo(List.of("Book", "User"));
        assertThat(bookUser.get().getJoinType()).isEqualTo(JoinType.MANY_TO_ONE);
        assertThat(bookUser.get().getCondition()).isEqualTo("Book.authorId = User.id");
        Assertions.assertThat(graphML.getRelationship("notfound")).isNotPresent();
    }

    @Test
    public void testListAndGetEnums()
    {
        assertThat(graphML.listEnums().size()).isEqualTo(1);
        Optional<EnumDefinition> enumField = graphML.getEnum("STATUS");
        assertThat(enumField).isPresent();
        assertThat(enumField.get().getValues()).isEqualTo(List.of("OK", "NOT_OK"));
        Assertions.assertThat(graphML.getEnum("notfound")).isNotPresent();
    }

    @Test
    public void testListAndGetMetrics()
    {
        assertThat(graphML.listMetrics().size()).isEqualTo(1);
        Optional<Metric> metric = graphML.getMetric("AuthorInfo");
        assertThat(metric.isPresent());
        assertThat(metric.get().getDimension().size()).isEqualTo(1);
        assertThat(metric.get().getMeasure().size()).isEqualTo(1);
        assertThat(metric.get().getBaseModel()).isEqualTo("Book");
    }
}
