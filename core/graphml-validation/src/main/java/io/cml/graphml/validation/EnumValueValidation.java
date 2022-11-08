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

package io.cml.graphml.validation;

import io.cml.graphml.GraphML;
import io.cml.graphml.connector.Client;
import io.cml.graphml.dto.EnumDefinition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.cml.graphml.validation.ValidationResult.error;
import static io.cml.graphml.validation.ValidationResult.fail;
import static io.cml.graphml.validation.ValidationResult.formatRuleWithIdentifier;
import static io.cml.graphml.validation.ValidationResult.pass;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;

public class EnumValueValidation
        extends ValidationRule
{
    public static final EnumValueValidation ENUM_VALUE_VALIDATION = new EnumValueValidation();
    private static final String RULE_PREFIX = "enum_";

    @Override
    public List<CompletableFuture<ValidationResult>> validate(Client client, GraphML graphML)
    {
        return graphML.listModels().stream()
                .flatMap(model ->
                        graphML.listEnums().stream()
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
            Iterator<Object[]> result = client.query(buildEnumCheck(refSql, columName, enumDefinition.getValues()));
            long elapsed = System.currentTimeMillis() - start;
            if (result.hasNext()) {
                Object[] row = result.next();
                if ((boolean) row[0]) {
                    return pass(formatRuleWithIdentifier(RULE_PREFIX + enumDefinition.getName(), modelName, columName), Duration.of(elapsed, ChronoUnit.MILLIS));
                }
                return fail(formatRuleWithIdentifier(RULE_PREFIX + enumDefinition.getName(), modelName, columName), Duration.of(elapsed, ChronoUnit.MILLIS), "Got null value in " + columName);
            }
            return error(formatRuleWithIdentifier(RULE_PREFIX + enumDefinition.getName(), modelName, columName), Duration.of(elapsed, ChronoUnit.MILLIS), "Query executed failed");
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
