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

package io.wren.main;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.wren.base.AnalyzedMDL;
import io.wren.base.WrenException;
import io.wren.base.sql.SqlConverter;
import io.wren.main.metadata.Metadata;
import io.wren.main.validation.ColumnIsValid;
import io.wren.main.validation.ValidationResult;
import io.wren.main.validation.ValidationRule;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static io.wren.base.metadata.StandardErrorCode.NOT_FOUND;
import static io.wren.main.validation.ColumnIsValid.COLUMN_IS_VALID;
import static java.util.Objects.requireNonNull;

public class ValidationService
{
    private final Map<String, ValidationRule> validationRules;

    @Inject
    public ValidationService(
            Metadata metadata,
            SqlConverter sqlConverter)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlConverter, "sqlConverter is null");
        this.validationRules = ImmutableMap.<String, ValidationRule>builder()
                .put(COLUMN_IS_VALID, new ColumnIsValid(metadata, sqlConverter))
                .build();
    }

    public CompletableFuture<List<ValidationResult>> validate(String ruleName, Map<String, Object> parameters, AnalyzedMDL analyzedMDL)
    {
        return CompletableFuture.supplyAsync(() -> Optional.ofNullable(validationRules.get(ruleName))
                .orElseThrow(() -> new WrenException(NOT_FOUND, "Validation rule not found: " + ruleName))
                .validate(analyzedMDL, parameters)
                .stream().map(CompletableFuture::join)
                .toList(), Executors.newVirtualThreadPerTaskExecutor());
    }
}
