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

package io.graphmdl;

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.GraphMDLTypes;
import io.graphmdl.base.dto.Column;
import io.graphmdl.base.dto.EnumDefinition;
import io.graphmdl.base.dto.JoinType;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.dto.Relationship;
import io.graphmdl.connector.Client;
import io.graphmdl.connector.duckdb.DuckdbClient;
import io.graphmdl.validation.DuplicateModelNameValidation;
import io.graphmdl.validation.EnumValueValidation;
import io.graphmdl.validation.MetricValidation;
import io.graphmdl.validation.ModelNameValidation;
import io.graphmdl.validation.ModelValidation;
import io.graphmdl.validation.NotNullValidation;
import io.graphmdl.validation.RelationshipValidation;
import io.graphmdl.validation.ValidationResult;
import org.testng.annotations.Test;

import java.util.List;

import static io.graphmdl.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetricValidation
{
    private final Client client;
    private final GraphMDL sample;
    private final String flightCsv = requireNonNull(getClass().getClassLoader().getResource("flight.csv")).getPath();

    public TestMetricValidation()
    {
        client = new DuckdbClient();
        sample = GraphMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("Flight",
                                format("SELECT * FROM '%s'", flightCsv),
                                List.of(
                                        Column.column("FlightDate", GraphMDLTypes.TIMESTAMP, null, true),
                                        Column.column("UniqueCarrier", "Carrier", null, true),
                                        Column.column("OriginCityName", GraphMDLTypes.VARCHAR, null, true),
                                        Column.column("DestCityName", GraphMDLTypes.VARCHAR, null, false),
                                        Column.column("Status", "Status", null, false)))))
                .setEnumDefinitions(List.of(
                        EnumDefinition.enumDefinition("Carrier", List.of("AA", "UA")),
                        EnumDefinition.enumDefinition("Status", List.of("OK", "NOT_OK"))))
                .build());
    }

    @Test
    public void testNotNullCheck()
    {
        List<ValidationResult> validationResults = MetricValidation.validate(client, sample, List.of(NotNullValidation.NOT_NULL));
        assertThat(validationResults.size()).isEqualTo(3);

        ValidationResult flightDate =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("not_null:Flight:FlightDate"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("not_null:Flight:FlightDate result is not found"));
        assertThat(flightDate.getStatus()).isEqualTo(ValidationResult.Status.PASS);
        assertThat(flightDate.getDuration()).isNotNull();
        assertThat(flightDate.getMessage()).isNull();

        ValidationResult uniqueCarrier =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("not_null:Flight:UniqueCarrier"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("not_null:Flight:UniqueCarrier result is not found"));
        assertThat(uniqueCarrier.getStatus()).isEqualTo(ValidationResult.Status.FAIL);
        assertThat(flightDate.getDuration()).isNotNull();
        assertThat(uniqueCarrier.getMessage()).isEqualTo("Got null value in UniqueCarrier");
    }

    @Test
    public void testEnumDefinition()
    {
        List<ValidationResult> validationResults = MetricValidation.validate(client, sample, List.of(EnumValueValidation.ENUM_VALUE_VALIDATION));
        assertThat(validationResults.size()).isEqualTo(2);

        ValidationResult enumUniqueCarrier =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("enum_Carrier:Flight:UniqueCarrier"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("enum_Carrier:Flight:UniqueCarrier result is not found"));
        assertThat(enumUniqueCarrier.getStatus()).isEqualTo(ValidationResult.Status.PASS);
        assertThat(enumUniqueCarrier.getDuration()).isNotNull();
        assertThat(enumUniqueCarrier.getMessage()).isNull();

        ValidationResult enumStatus =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("enum_Status:Flight:Status"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("enum_Status:Flight:Status result is not found"));

        assertThat(enumStatus.getStatus()).isEqualTo(ValidationResult.Status.FAIL);
        assertThat(enumStatus.getDuration()).isNotNull();
        assertThat(enumStatus.getMessage()).isEqualTo("Got invalid enum value in Status");
    }

    @Test
    public void testModelValidation()
    {
        GraphMDL wrongManifest = GraphMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("Flight",
                                format("SELECT * FROM '%s'", flightCsv),
                                List.of(
                                        Column.column("FlightDate", GraphMDLTypes.TIMESTAMP, null, true),
                                        Column.column("illegal^name", GraphMDLTypes.VARCHAR, null, true),
                                        Column.column("123illegalname", GraphMDLTypes.VARCHAR, null, true),
                                        Column.column("notfound", GraphMDLTypes.VARCHAR, null, true),
                                        Column.column("A", GraphMDLTypes.INTEGER, null, false)))))
                .build());

        List<ValidationResult> validationResults = MetricValidation.validate(client, wrongManifest, List.of(ModelValidation.MODEL_VALIDATION));
        assertThat(validationResults.size()).isEqualTo(1);

        ValidationResult result = validationResults.get(0);
        assertThat(result.getStatus()).isEqualTo(ValidationResult.Status.FAIL);
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
            List<ValidationResult> validationResults = MetricValidation.validate(client, sample, List.of(DuplicateModelNameValidation.DUPLICATE_MODEL_NAME_VALIDATION));
            assertThat(validationResults.size()).isEqualTo(1);

            ValidationResult validationResult = validationResults.get(0);
            assertThat(validationResult.getName()).isEqualTo("duplicate_model_name");
            assertThat(validationResult.getDuration()).isNotNull();
            assertThat(validationResult.getStatus()).isEqualTo(ValidationResult.Status.FAIL);
            assertThat(validationResult.getMessage()).isEqualTo("Find duplicate table name in the remote data source. Duplicate table: Flight");
        }
        finally {
            client.executeDDL("DROP TABLE Flight");
        }
    }

    @Test
    public void testInvalidModelNameValidation()
    {
        GraphMDL invalidModels = GraphMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model("123Flight",
                                format("SELECT * FROM '%s'", flightCsv),
                                List.of(
                                        Column.column("FlightDate", GraphMDLTypes.TIMESTAMP, null, true))),
                        Model.model("Fl^ight",
                                format("SELECT * FROM '%s'", flightCsv),
                                List.of(
                                        Column.column("FlightDate", GraphMDLTypes.TIMESTAMP, null, true))),
                        Model.model("_Flight",
                                format("SELECT * FROM '%s'", flightCsv),
                                List.of(
                                        Column.column("FlightDate", GraphMDLTypes.TIMESTAMP, null, true)))))
                .build());
        List<ValidationResult> validationResults = MetricValidation.validate(client, invalidModels, List.of(ModelNameValidation.MODEL_NAME_VALIDATION));
        assertThat(validationResults.size()).isEqualTo(1);
        ValidationResult first = validationResults.get(0);
        assertThat(first.getName()).isEqualTo("model_name");
        assertThat(first.getStatus()).isEqualTo(ValidationResult.Status.FAIL);
        assertThat(first.getDuration()).isNotNull();
        assertThat(first.getMessage()).isEqualTo("Find invalid model name: 123Flight,Fl^ight");
    }

    @Test
    public void testRelationshipValidation()
    {
        String bookCsv = requireNonNull(getClass().getClassLoader().getResource("book.csv")).getPath();
        String userCsv = requireNonNull(getClass().getClassLoader().getResource("user.csv")).getPath();

        GraphMDL graphMDL = GraphMDL.fromManifest(withDefaultCatalogSchema()
                .setModels(List.of(
                        Model.model(
                                "Book",
                                format("SELECT * FROM '%s'", bookCsv),
                                List.of(
                                        Column.column("name", GraphMDLTypes.VARCHAR, null, false),
                                        Column.column("author", GraphMDLTypes.VARCHAR, "User", false))),
                        Model.model("User",
                                format("SELECT * FROM '%s'", userCsv),
                                List.of(
                                        Column.column("id", GraphMDLTypes.INTEGER, null, false),
                                        Column.column("name", GraphMDLTypes.VARCHAR, null, false),
                                        Column.column("email", GraphMDLTypes.VARCHAR, null, false)))))
                .setRelationships(List.of(
                        Relationship.relationship("BookUserOneToOne", List.of("Book", "User"), JoinType.ONE_TO_ONE, "Book.authorId = User.id"),
                        Relationship.relationship("BookUserOneToMany", List.of("Book", "User"), JoinType.ONE_TO_MANY, "Book.authorId = User.id"),
                        Relationship.relationship("BookUserManyToOne", List.of("Book", "User"), JoinType.MANY_TO_ONE, "Book.authorId = User.id"),
                        Relationship.relationship("BookUserManyToMany", List.of("Book", "User"), JoinType.MANY_TO_MANY, "Book.authorId = User.id"),
                        Relationship.relationship("FakeBookOneToOne", List.of("Book", "User"), JoinType.ONE_TO_ONE, "Book.fakeId = User.id"),
                        Relationship.relationship("FakeBookManyToOne", List.of("Book", "User"), JoinType.MANY_TO_ONE, "Book.fakeId = User.id"),
                        Relationship.relationship("FakeBookOneToMany", List.of("Book", "User"), JoinType.ONE_TO_MANY, "Book.fakeId = User.id"),
                        Relationship.relationship("FakeBookManyToMany", List.of("Book", "User"), JoinType.MANY_TO_MANY, "Book.fakeId = User.id"),
                        Relationship.relationship("FakeUserOneToOne", List.of("Book", "User"), JoinType.ONE_TO_ONE, "Book.authorId = User.fakeId"),
                        Relationship.relationship("FakeUserManyToOne", List.of("Book", "User"), JoinType.MANY_TO_ONE, "Book.authorId = User.fakeId"),
                        Relationship.relationship("FakeUserOneToMany", List.of("Book", "User"), JoinType.ONE_TO_MANY, "Book.authorId = User.fakeId"),
                        Relationship.relationship("FakeUserManyToMany", List.of("Book", "User"), JoinType.MANY_TO_MANY, "Book.authorId = User.fakeId"),
                        Relationship.relationship("FakeBookUserOneToOne", List.of("Book", "User"), JoinType.ONE_TO_ONE, "Book.fakeId = User.fakeId"),
                        Relationship.relationship("FakeBookUserManyToOne", List.of("Book", "User"), JoinType.MANY_TO_ONE, "Book.fakeId = User.fakeId"),
                        Relationship.relationship("FakeBookUserOneToMany", List.of("Book", "User"), JoinType.ONE_TO_MANY, "Book.fakeId = User.fakeId"),
                        Relationship.relationship("FakeBookUserManyToMany", List.of("Book", "User"), JoinType.MANY_TO_MANY, "Book.fakeId = User.fakeId"),
                        Relationship.relationship("NotFoundModelInCondition", List.of("Book", "User"), JoinType.ONE_TO_ONE, "notfound.id = wrong.id")))
                .build());

        List<ValidationResult> validationResults = MetricValidation.validate(client, graphMDL, List.of(RelationshipValidation.RELATIONSHIP_VALIDATION));
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
        assertThat(validationResult.getStatus()).isEqualTo(ValidationResult.Status.FAIL);
        assertThat(validationResult.getDuration()).isNotNull();
        assertThat(validationResult.getMessage()).isEqualTo("notfound model is not found");
    }

    private void assertRelationshipPassed(String name, List<ValidationResult> results)
    {
        ValidationResult validationResult = results.stream().filter(result -> result.getName().equals(name)).findAny()
                .orElseThrow(() -> new AssertionError(format("%s result is not found", name)));
        assertThat(validationResult.getStatus()).isEqualTo(ValidationResult.Status.PASS);
        assertThat(validationResult.getDuration()).isNotNull();
        assertThat(validationResult.getMessage()).isNull();
    }

    private void assertRelationshipFailed(String name, List<String> wrongTables, List<ValidationResult> results)
    {
        ValidationResult validationResult = results.stream().filter(result -> result.getName().equals(name)).findAny()
                .orElseThrow(() -> new AssertionError(format("%s result is not found", name)));
        assertThat(validationResult.getStatus()).isEqualTo(ValidationResult.Status.FAIL);
        assertThat(validationResult.getDuration()).isNotNull();
        assertThat(validationResult.getMessage()).isEqualTo("Got duplicate join key in " + join(",", wrongTables));
    }
}
