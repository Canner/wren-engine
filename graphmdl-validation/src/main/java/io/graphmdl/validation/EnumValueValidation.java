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
import io.graphmdl.base.client.AutoCloseableIterator;
import io.graphmdl.base.client.Client;
import io.graphmdl.base.dto.EnumDefinition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;

public class EnumValueValidation
        extends ValidationRule
{
    public static final EnumValueValidation ENUM_VALUE_VALIDATION = new EnumValueValidation();
    private static final String RULE_PREFIX = "enum_";

    @Override
    public List<CompletableFuture<ValidationResult>> validate(Client client, GraphMDL graphMDL)
    {
        return graphMDL.listModels().stream()
                .flatMap(model ->
                        graphMDL.listEnums().stream()
                                .flatMap(enumDefinition ->
                                        model.getColumns().stream()
                                                .filter(column -> column.getType().equals(enumDefinition.getName()))
                                                .map(column -> validateColumn(client, model.getRefSql(), model.getName(), column.getName(), enumDefinition))))
                .collect(toUnmodifiableList());
    }

    public CompletableFuture<ValidationResult> validateColumn(Client client, String refSql, String modelName, String columName, EnumDefinition enumDefinition)
    {
        return CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            try (AutoCloseableIterator<Object[]> result = client.query(buildEnumCheck(refSql, columName, enumDefinition.getValues()))) {
                long elapsed = System.currentTimeMillis() - start;
                if (result.hasNext()) {
                    Object[] row = result.next();
                    if ((boolean) row[0]) {
                        return ValidationResult.pass(ValidationResult.formatRuleWithIdentifier(RULE_PREFIX + enumDefinition.getName(), modelName, columName), Duration.of(elapsed, ChronoUnit.MILLIS));
                    }
                    return ValidationResult.fail(ValidationResult.formatRuleWithIdentifier(RULE_PREFIX + enumDefinition.getName(), modelName, columName), Duration.of(elapsed, ChronoUnit.MILLIS), "Got invalid enum value in " + columName);
                }
                return ValidationResult.error(ValidationResult.formatRuleWithIdentifier(RULE_PREFIX + enumDefinition.getName(), modelName, columName), Duration.of(elapsed, ChronoUnit.MILLIS), "Query executed failed");
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String buildEnumCheck(String refSql, String columnName, List<String> enumValues)
    {
        String enumValueString = enumValues.stream().map(EnumValueValidation::singleQuoted).collect(joining(","));
        return format("WITH source AS (%s) SELECT count(*) = 0 FROM source WHERE %s NOT IN (%s) AND %s IS NOT NULL", refSql, columnName, enumValueString, columnName);
    }

    private static String singleQuoted(String value)
    {
        return "'" + value + "'";
    }
}
