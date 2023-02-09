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

import io.graphmdl.base.GraphML;
import io.graphmdl.base.dto.EnumDefinition;
import io.graphmdl.base.dto.Model;
import io.graphmdl.connector.Client;
import io.graphmdl.connector.ColumnDescription;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;

public class ModelValidation
        extends ValidationRule
{
    public static final ModelValidation MODEL_VALIDATION = new ModelValidation();
    private static final String RULE_NAME = "model";
    private static final Pattern COLUMN_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*");

    @Override
    public List<CompletableFuture<ValidationResult>> validate(Client client, GraphML graphML)
    {
        return graphML.listModels().stream()
                .map(model -> validateModel(client, model, graphML.listEnums()))
                .collect(toUnmodifiableList());
    }

    private CompletableFuture<ValidationResult> validateModel(Client client, Model model, List<EnumDefinition> enumDefinitions)
    {
        return CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            Iterable<ColumnDescription> result = () -> client.describe(buildSql(model.getRefSql()));
            List<ColumnDescription> columnDescriptions = StreamSupport.stream(result.spliterator(), false).collect(toUnmodifiableList());

            if (columnDescriptions.isEmpty()) {
                return ValidationResult.error(ValidationResult.formatRuleWithIdentifier(RULE_NAME, model.getName()),
                        Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS), "Query executed failed or no result");
            }

            List<ColumnValidationFailed> validationFaileds = model.getColumns().stream().map(modelColumn -> {
                Matcher matcher = COLUMN_NAME_PATTERN.matcher(modelColumn.getName());
                if (!matcher.matches()) {
                    return new ColumnValidationFailed(modelColumn.getName(), "Illegal column name");
                }
                Optional<ColumnDescription> descriptionOptional = columnDescriptions.stream()
                        .filter(columnDescription -> columnDescription.getName().equals(modelColumn.getName())).findAny();
                if (descriptionOptional.isEmpty()) {
                    return new ColumnValidationFailed(modelColumn.getName(), format("Can't be found in model %s", model.getName()));
                }

                Optional<EnumDefinition> isEnumType = enumDefinitions.stream().filter(enumDefinition -> enumDefinition.getName().equals(modelColumn.getType())).findAny();
                if (isEnumType.isEmpty() && !descriptionOptional.get().getType().equals(modelColumn.getType())) {
                    return new ColumnValidationFailed(modelColumn.getName(),
                            format("Got incompatible type in column %s. Expected %s but actual %s", modelColumn.getName(), modelColumn.getType(), descriptionOptional.get().getType()));
                }
                return null;
            }).filter(Objects::nonNull).collect(toUnmodifiableList());

            if (validationFaileds.isEmpty()) {
                return ValidationResult.pass(ValidationResult.formatRuleWithIdentifier(RULE_NAME, model.getName()), Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS));
            }

            return ValidationResult.fail(ValidationResult.formatRuleWithIdentifier(RULE_NAME, model.getName()),
                    Duration.of(System.currentTimeMillis() - start, ChronoUnit.MILLIS), validationFaileds.stream().map(ColumnValidationFailed::toString).collect(joining(",")));
        });
    }

    private String buildSql(String refSql)
    {
        return format("WITH source AS (%s) SELECT * FROM source", refSql);
    }

    static class ColumnValidationFailed
    {
        private final String name;
        private final String errorMessage;

        public ColumnValidationFailed(String name, String errorMessage)
        {
            this.name = name;
            this.errorMessage = errorMessage;
        }

        @Override
        public String toString()
        {
            return format("[%s:%s]", name, errorMessage);
        }
    }
}
