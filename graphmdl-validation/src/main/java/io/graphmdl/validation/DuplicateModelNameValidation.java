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

package io.graphmdl.validation;

import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.dto.Model;
import io.graphmdl.base.client.Client;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toUnmodifiableList;

public class DuplicateModelNameValidation
        extends ValidationRule
{
    public static final DuplicateModelNameValidation DUPLICATE_MODEL_NAME_VALIDATION = new DuplicateModelNameValidation();
    private static final String RULE_NAME = "duplicate_model_name";

    @Override
    public List<CompletableFuture<ValidationResult>> validate(Client client, GraphMDL graphMDL)
    {
        return List.of(CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            List<String> tableNames = client.listTables();
            List<String> duplicateTable = graphMDL.listModels().stream().map(Model::getName).filter(tableNames::contains).collect(toUnmodifiableList());
            if (duplicateTable.isEmpty()) {
                return ValidationResult.pass(RULE_NAME, Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS));
            }

            return ValidationResult.fail(RULE_NAME, Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS),
                    format("Find duplicate table name in the remote data source. Duplicate table: %s", join(",", duplicateTable)));
        }));
    }
}
