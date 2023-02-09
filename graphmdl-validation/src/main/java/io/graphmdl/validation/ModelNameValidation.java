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
import io.graphmdl.connector.Client;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toUnmodifiableList;

public class ModelNameValidation
        extends ValidationRule
{
    public static final ModelNameValidation MODEL_NAME_VALIDATION = new ModelNameValidation();
    private static final String RULE_NAME = "model_name";

    private static final Pattern MODEL_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*");

    @Override
    public List<CompletableFuture<ValidationResult>> validate(Client client, GraphMDL graphMDL)
    {
        return List.of(CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            List<String> invalidNames = graphMDL.listModels().stream()
                    .map(Model::getName)
                    .filter(name -> !MODEL_NAME_PATTERN.matcher(name).matches())
                    .collect(toUnmodifiableList());
            if (invalidNames.isEmpty()) {
                return ValidationResult.pass(RULE_NAME, Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS));
            }

            return ValidationResult.fail(RULE_NAME, Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS),
                    format("Find invalid model name: %s", join(",", invalidNames)));
        }));
    }
}
