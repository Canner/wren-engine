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

import io.cml.graphml.base.GraphML;
import io.cml.graphml.base.GraphMLTypes;
import io.cml.graphml.base.dto.JoinType;
import io.cml.graphml.connector.Client;
import io.cml.graphml.connector.duckdb.DuckdbClient;
import io.cml.graphml.validation.MetricValidation;
import io.cml.graphml.validation.ValidationResult;
import org.testng.annotations.Test;

import java.util.List;

import static io.cml.graphml.base.dto.Column.column;
import static io.cml.graphml.base.dto.EnumDefinition.enumDefinition;
import static io.cml.graphml.base.dto.Manifest.manifest;
import static io.cml.graphml.base.dto.Model.model;
import static io.cml.graphml.base.dto.Relationship.relationship;
import static io.cml.graphml.validation.DuplicateModelNameValidation.DUPLICATE_MODEL_NAME_VALIDATION;
import static io.cml.graphml.validation.EnumValueValidation.ENUM_VALUE_VALIDATION;
import static io.cml.graphml.validation.ModelNameValidation.MODEL_NAME_VALIDATION;
import static io.cml.graphml.validation.ModelValidation.MODEL_VALIDATION;
import static io.cml.graphml.validation.NotNullValidation.NOT_NULL;
import static io.cml.graphml.validation.RelationshipValidation.RELATIONSHIP_VALIDATION;
import static io.cml.graphml.validation.ValidationResult.Status.FAIL;
import static io.cml.graphml.validation.ValidationResult.Status.PASS;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetricValidation
{
    private final Client client;
    private final GraphML sample;
    private final String flightCsv = requireNonNull(getClass().getClassLoader().getResource("flight.csv")).getPath();

    public TestMetricValidation()
    {
        client = new DuckdbClient();
        sample = GraphML.fromManifest(manifest(
                List.of(model("Flight",
                        format("SELECT * FROM '%s'", flightCsv),
                        List.of(
                                column("FlightDate", GraphMLTypes.TIMESTAMP, null, true),
                                column("UniqueCarrier", "Carrier", null, true),
                                column("OriginCityName", GraphMLTypes.VARCHAR, null, true),
                                column("DestCityName", GraphMLTypes.VARCHAR, null, false),
                                column("Status", "Status", null, false)))),
                List.of(),
                List.of(
                        enumDefinition("Carrier", List.of("AA", "UA")),
                        enumDefinition("Status", List.of("OK", "NOT_OK"))),
                List.of()));
    }

    @Test
    public void testNotNullCheck()
    {
        List<ValidationResult> validationResults = MetricValidation.validate(client, sample, List.of(NOT_NULL));
        assertThat(validationResults.size()).isEqualTo(3);

        ValidationResult flightDate =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("not_null:Flight:FlightDate"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("not_null:Flight:FlightDate result is not found"));
        assertThat(flightDate.getStatus()).isEqualTo(PASS);
        assertThat(flightDate.getDuration()).isNotNull();
        assertThat(flightDate.getMessage()).isNull();

        ValidationResult uniqueCarrier =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("not_null:Flight:UniqueCarrier"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("not_null:Flight:UniqueCarrier result is not found"));
        assertThat(uniqueCarrier.getStatus()).isEqualTo(FAIL);
        assertThat(flightDate.getDuration()).isNotNull();
        assertThat(uniqueCarrier.getMessage()).isEqualTo("Got null value in UniqueCarrier");
    }

    @Test
    public void testEnumDefinition()
    {
        List<ValidationResult> validationResults = MetricValidation.validate(client, sample, List.of(ENUM_VALUE_VALIDATION));
        assertThat(validationResults.size()).isEqualTo(2);

        ValidationResult enumUniqueCarrier =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("enum_Carrier:Flight:UniqueCarrier"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("enum_Carrier:Flight:UniqueCarrier result is not found"));
        assertThat(enumUniqueCarrier.getStatus()).isEqualTo(PASS);
        assertThat(enumUniqueCarrier.getDuration()).isNotNull();
        assertThat(enumUniqueCarrier.getMessage()).isNull();

        ValidationResult enumStatus =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("enum_Status:Flight:Status"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("enum_Status:Flight:Status result is not found"));

        assertThat(enumStatus.getStatus()).isEqualTo(FAIL);
        assertThat(enumStatus.getDuration()).isNotNull();
        assertThat(enumStatus.getMessage()).isEqualTo("Got invalid enum value in Status");
    }

    @Test
    public void testModelValidation()
    {
        GraphML wrongManifest = GraphML.fromManifest(manifest(
                List.of(model("Flight",
                        format("SELECT * FROM '%s'", flightCsv),
                        List.of(
                                column("FlightDate", GraphMLTypes.TIMESTAMP, null, true),
                                column("illegal^name", GraphMLTypes.VARCHAR, null, true),
                                column("123illegalname", GraphMLTypes.VARCHAR, null, true),
                                column("notfound", GraphMLTypes.VARCHAR, null, true),
                                column("A", GraphMLTypes.INTEGER, null, false)))),
                List.of(),
                List.of(),
                List.of()));

        List<ValidationResult> validationResults = MetricValidation.validate(client, wrongManifest, List.of(MODEL_VALIDATION));
        assertThat(validationResults.size()).isEqualTo(1);

        ValidationResult result = validationResults.get(0);
        assertThat(result.getStatus()).isEqualTo(FAIL);
        assertThat(result.getName()).isEqualTo("model:Flight");
        assertThat(result.getDuration()).isNotNull();
        assertThat(result.getMessage()).isNotNull();
        String[] errorMessage = result.getMessage().split(",");
        assertThat(errorMessage.length).isEqualTo(4);
        assertThat(errorMessage[0]).isEqualTo("[FlightDate:Got incompatible type in column FlightDate. Expected timestamp but actual varchar]");
        assertThat(errorMessage[1]).isEqualTo("[illegal^name:Illegal column name]");
        assertThat(errorMessage[2]).isEqualTo("[123illegalname:Illegal column name]");
        assertThat(errorMessage[3]).isEqualTo("[notfound:Can't be found in model Flight]");
    }

    @Test
    public void testDuplicateModelNameValidation()
    {
        try {
            client.executeDDL(format("CREATE TABLE Flight AS SELECT * FROM '%s'", flightCsv));
            List<ValidationResult> validationResults = MetricValidation.validate(client, sample, List.of(DUPLICATE_MODEL_NAME_VALIDATION));
            assertThat(validationResults.size()).isEqualTo(1);

            ValidationResult validationResult = validationResults.get(0);
            assertThat(validationResult.getName()).isEqualTo("duplicate_model_name");
            assertThat(validationResult.getDuration()).isNotNull();
            assertThat(validationResult.getStatus()).isEqualTo(FAIL);
            assertThat(validationResult.getMessage()).isEqualTo("Find duplicate table name in the remote data source. Duplicate table: Flight");
        }
        finally {
            client.executeDDL("DROP TABLE Flight");
        }
    }

    @Test
    public void testInvalidModelNameValidation()
    {
        GraphML invalidModels = GraphML.fromManifest(manifest(
                List.of(model("123Flight",
                                format("SELECT * FROM '%s'", flightCsv),
                                List.of(
                                        column("FlightDate", GraphMLTypes.TIMESTAMP, null, true))),
                        model("Fl^ight",
                                format("SELECT * FROM '%s'", flightCsv),
                                List.of(
                                        column("FlightDate", GraphMLTypes.TIMESTAMP, null, true))),
                        model("_Flight",
                                format("SELECT * FROM '%s'", flightCsv),
                                List.of(
                                        column("FlightDate", GraphMLTypes.TIMESTAMP, null, true)))),
                List.of(),
                List.of(),
                List.of()));
        List<ValidationResult> validationResults = MetricValidation.validate(client, invalidModels, List.of(MODEL_NAME_VALIDATION));
        assertThat(validationResults.size()).isEqualTo(1);
        ValidationResult first = validationResults.get(0);
        assertThat(first.getName()).isEqualTo("model_name");
        assertThat(first.getStatus()).isEqualTo(FAIL);
        assertThat(first.getDuration()).isNotNull();
        assertThat(first.getMessage()).isEqualTo("Find invalid model name: 123Flight,Fl^ight");
    }

    @Test
    public void testRelationshipValidation()
    {
        String bookCsv = requireNonNull(getClass().getClassLoader().getResource("book.csv")).getPath();
        String userCsv = requireNonNull(getClass().getClassLoader().getResource("user.csv")).getPath();

        GraphML graphML = GraphML.fromManifest(manifest(
                List.of(model(
                                "Book",
                                format("SELECT * FROM '%s'", bookCsv),
                                List.of(
                                        column("name", GraphMLTypes.VARCHAR, null, false),
                                        column("author", GraphMLTypes.VARCHAR, "User", false))),
                        model("User",
                                format("SELECT * FROM '%s'", userCsv),
                                List.of(
                                        column("id", GraphMLTypes.INTEGER, null, false),
                                        column("name", GraphMLTypes.VARCHAR, null, false),
                                        column("email", GraphMLTypes.VARCHAR, null, false)))),
                List.of(
                        relationship("BookUserOneToOne", List.of("Book", "User"), JoinType.ONE_TO_ONE, "Book.authorId = User.id"),
                        relationship("BookUserOneToMany", List.of("Book", "User"), JoinType.ONE_TO_MANY, "Book.authorId = User.id"),
                        relationship("BookUserManyToOne", List.of("Book", "User"), JoinType.MANY_TO_ONE, "Book.authorId = User.id"),
                        relationship("BookUserManyToMany", List.of("Book", "User"), JoinType.MANY_TO_MANY, "Book.authorId = User.id"),
                        relationship("FakeBookOneToOne", List.of("Book", "User"), JoinType.ONE_TO_ONE, "Book.fakeId = User.id"),
                        relationship("FakeBookManyToOne", List.of("Book", "User"), JoinType.MANY_TO_ONE, "Book.fakeId = User.id"),
                        relationship("FakeBookOneToMany", List.of("Book", "User"), JoinType.ONE_TO_MANY, "Book.fakeId = User.id"),
                        relationship("FakeBookManyToMany", List.of("Book", "User"), JoinType.MANY_TO_MANY, "Book.fakeId = User.id"),
                        relationship("FakeUserOneToOne", List.of("Book", "User"), JoinType.ONE_TO_ONE, "Book.authorId = User.fakeId"),
                        relationship("FakeUserManyToOne", List.of("Book", "User"), JoinType.MANY_TO_ONE, "Book.authorId = User.fakeId"),
                        relationship("FakeUserOneToMany", List.of("Book", "User"), JoinType.ONE_TO_MANY, "Book.authorId = User.fakeId"),
                        relationship("FakeUserManyToMany", List.of("Book", "User"), JoinType.MANY_TO_MANY, "Book.authorId = User.fakeId"),
                        relationship("FakeBookUserOneToOne", List.of("Book", "User"), JoinType.ONE_TO_ONE, "Book.fakeId = User.fakeId"),
                        relationship("FakeBookUserManyToOne", List.of("Book", "User"), JoinType.MANY_TO_ONE, "Book.fakeId = User.fakeId"),
                        relationship("FakeBookUserOneToMany", List.of("Book", "User"), JoinType.ONE_TO_MANY, "Book.fakeId = User.fakeId"),
                        relationship("FakeBookUserManyToMany", List.of("Book", "User"), JoinType.MANY_TO_MANY, "Book.fakeId = User.fakeId"),
                        relationship("NotFoundModelInCondition", List.of("Book", "User"), JoinType.ONE_TO_ONE, "notfound.id = wrong.id")),
                List.of(),
                List.of()));

        List<ValidationResult> validationResults = MetricValidation.validate(client, graphML, List.of(RELATIONSHIP_VALIDATION));
        assertThat(validationResults.size()).isEqualTo(17);
        assertRelationshipPassed("relationship_ONE_TO_ONE:BookUserOneToOne", validationResults);
        assertRelationshipPassed("relationship_ONE_TO_MANY:BookUserOneToMany", validationResults);
        assertRelationshipPassed("relationship_MANY_TO_ONE:BookUserManyToOne", validationResults);
        assertRelationshipPassed("relationship_MANY_TO_MANY:BookUserManyToMany", validationResults);

        assertRelationshipPassed("relationship_MANY_TO_ONE:FakeBookManyToOne", validationResults);
        assertRelationshipPassed("relationship_MANY_TO_MANY:FakeBookManyToMany", validationResults);
        assertRelationshipFailed("relationship_ONE_TO_ONE:FakeBookOneToOne", List.of("Book"), validationResults);
        assertRelationshipFailed("relationship_ONE_TO_MANY:FakeBookOneToMany", List.of("Book"), validationResults);

        assertRelationshipPassed("relationship_ONE_TO_MANY:FakeUserOneToMany", validationResults);
        assertRelationshipPassed("relationship_MANY_TO_MANY:FakeUserManyToMany", validationResults);
        assertRelationshipFailed("relationship_ONE_TO_ONE:FakeUserOneToOne", List.of("User"), validationResults);
        assertRelationshipFailed("relationship_MANY_TO_ONE:FakeUserManyToOne", List.of("User"), validationResults);

        assertRelationshipPassed("relationship_MANY_TO_MANY:FakeBookUserManyToMany", validationResults);
        assertRelationshipFailed("relationship_ONE_TO_ONE:FakeBookUserOneToOne", List.of("Book", "User"), validationResults);
        assertRelationshipFailed("relationship_MANY_TO_ONE:FakeBookUserManyToOne", List.of("User"), validationResults);
        assertRelationshipFailed("relationship_ONE_TO_MANY:FakeBookUserOneToMany", List.of("Book"), validationResults);

        String name = "relationship_ONE_TO_ONE:NotFoundModelInCondition";
        ValidationResult validationResult = validationResults.stream().filter(result -> result.getName().equals(name)).findAny()
                .orElseThrow(() -> new AssertionError(format("%s result is not found", name)));
        assertThat(validationResult.getStatus()).isEqualTo(FAIL);
        assertThat(validationResult.getDuration()).isNotNull();
        assertThat(validationResult.getMessage()).isEqualTo("notfound model is not found");
    }

    private void assertRelationshipPassed(String name, List<ValidationResult> results)
    {
        ValidationResult validationResult = results.stream().filter(result -> result.getName().equals(name)).findAny()
                .orElseThrow(() -> new AssertionError(format("%s result is not found", name)));
        assertThat(validationResult.getStatus()).isEqualTo(PASS);
        assertThat(validationResult.getDuration()).isNotNull();
        assertThat(validationResult.getMessage()).isNull();
    }

    private void assertRelationshipFailed(String name, List<String> wrongTables, List<ValidationResult> results)
    {
        ValidationResult validationResult = results.stream().filter(result -> result.getName().equals(name)).findAny()
                .orElseThrow(() -> new AssertionError(format("%s result is not found", name)));
        assertThat(validationResult.getStatus()).isEqualTo(FAIL);
        assertThat(validationResult.getDuration()).isNotNull();
        assertThat(validationResult.getMessage()).isEqualTo("Got duplicate join key in " + join(",", wrongTables));
    }
}
