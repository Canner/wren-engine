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

import io.cml.graphml.connector.Client;
import io.cml.graphml.connector.duckdb.DuckdbClient;
import io.cml.graphml.validation.MetricValidation;
import io.cml.graphml.validation.ValidationResult;
import org.testng.annotations.Test;

import java.util.List;

import static io.cml.graphml.dto.Column.column;
import static io.cml.graphml.dto.EnumDefinition.enumDefinition;
import static io.cml.graphml.dto.Manifest.manifest;
import static io.cml.graphml.dto.Model.model;
import static io.cml.graphml.validation.EnumValueValidation.ENUM_VALUE_VALIDATION;
import static io.cml.graphml.validation.ModelValidation.MODEL_VALIDATION;
import static io.cml.graphml.validation.NotNullValidation.NOT_NULL;
import static io.cml.graphml.validation.ValidationResult.Status.FAIL;
import static io.cml.graphml.validation.ValidationResult.Status.PASS;
import static java.lang.String.format;
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
                        enumDefinition("Status", List.of("OK", "NOT_OK")))));
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
                                column("notfound", GraphMLTypes.VARCHAR, null, true)))),
                List.of(),
                List.of()));

        List<ValidationResult> validationResults = MetricValidation.validate(client, wrongManifest, List.of(MODEL_VALIDATION));
        assertThat(validationResults.size()).isEqualTo(1);

        String[] errorMessage = validationResults.get(0).getMessage().split(",");
        assertThat(errorMessage[0]).isEqualTo("[FlightDate:Got incompatible type in column FlightDate. Expected timestamp but actual varchar]");
        assertThat(errorMessage[1]).isEqualTo("[illegal^name:Column name contains illegal character]");
        assertThat(errorMessage[2]).isEqualTo("[notfound:Can't be found in model Flight]");
    }
}
