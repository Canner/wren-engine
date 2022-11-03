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
import static io.cml.graphml.dto.Manifest.manifest;
import static io.cml.graphml.dto.Model.model;
import static io.cml.graphml.dto.Type.STRING;
import static io.cml.graphml.validation.ValidationResult.Status.ERROR;
import static io.cml.graphml.validation.ValidationResult.Status.PASS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetricValidation
{
    private final Client client;
    private final GraphML sample;

    public TestMetricValidation()
    {
        String flightCsv = requireNonNull(getClass().getClassLoader().getResource("flight.csv")).getPath();
        client = new DuckdbClient();
        sample = GraphML.fromManifest(manifest(
                List.of(model("Flight",
                        format("SELECT * FROM '%s'", flightCsv),
                        List.of(
                                column("FlightDate", STRING, null, true),
                                column("UniqueCarrier", STRING, null, true),
                                column("OriginCityName", STRING, null, true),
                                column("DestCityName", STRING, null, false)))),
                List.of()));
    }

    @Test
    public void testNotNullCheck()
    {
        List<ValidationResult> validationResults = MetricValidation.validate(client, sample);
        assertThat(validationResults.size()).isEqualTo(3);

        ValidationResult flightDate =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("not_null-FlightDate"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("not_null-FlightDate result is not found"));
        assertThat(flightDate.getStatus()).isEqualTo(PASS);
        assertThat(flightDate.getDuration()).isNotNull();
        assertThat(flightDate.getMessage()).isNull();

        ValidationResult uniqueCarrier =
                validationResults.stream().filter(validationResult -> validationResult.getName().equals("not_null-UniqueCarrier"))
                        .findAny()
                        .orElseThrow(() -> new AssertionError("not_null-UniqueCarrier result is not found"));
        assertThat(uniqueCarrier.getStatus()).isEqualTo(ERROR);
        assertThat(flightDate.getDuration()).isNotNull();
        assertThat(uniqueCarrier.getMessage()).isEqualTo("Got null value in UniqueCarrier");
    }
}
