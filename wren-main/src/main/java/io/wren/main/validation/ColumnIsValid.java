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

package io.wren.main.validation;

import io.airlift.units.Duration;
import io.wren.base.AnalyzedMDL;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.SessionContext;
import io.wren.base.sql.SqlConverter;
import io.wren.base.sqlrewrite.WrenPlanner;
import io.wren.main.metadata.Metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static io.wren.main.validation.ValidationResult.formatRuleWithIdentifier;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ColumnIsValid
        implements ValidationRule
{
    public static final String COLUMN_IS_VALID = "column_is_valid";
    private static final String MODEL_NAME = "modelName";
    private static final String COLUMN_NAME = "columnName";

    public static Map<String, Object> parameters(String model, String column)
    {
        Map<String, Object> map = new HashMap<>();
        map.put(MODEL_NAME, model);
        map.put(COLUMN_NAME, column);
        return map;
    }

    private final Metadata metadata;
    private final SqlConverter sqlConverter;

    public ColumnIsValid(Metadata metadata, SqlConverter sqlConverter)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
    }

    @Override
    public String getRuleName()
    {
        return COLUMN_IS_VALID;
    }

    @Override
    public List<CompletableFuture<ValidationResult>> validate(AnalyzedMDL analyzedMDL, Map<String, Object> parameters)
    {
        return List.of(CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            Optional<String> modelName = getModelName(parameters);
            Optional<String> columnName = getColumnName(parameters);

            if (modelName.isEmpty() || modelName.get().isEmpty()) {
                return ValidationResult.error(getRuleName(), Duration.succinctDuration(System.currentTimeMillis() - start, MILLISECONDS), "Model name is required");
            }

            if (columnName.isEmpty() || columnName.get().isEmpty()) {
                return ValidationResult.error(formatRuleWithIdentifier(getRuleName(), modelName.get()),
                        Duration.succinctDuration(System.currentTimeMillis() - start, MILLISECONDS), "Column name is required");
            }

            try {
                String sql = format("""
                        SELECT "%s" FROM "%s" LIMIT 1""", columnName.get(), modelName.get());
                SessionContext sessionContext = SessionContext.builder()
                        .setCatalog(analyzedMDL.getWrenMDL().getCatalog())
                        .setSchema(analyzedMDL.getWrenMDL().getSchema())
                        .setEnableDynamic(true)
                        .build();
                String planned = WrenPlanner.rewrite(sql, sessionContext, analyzedMDL);
                String converted = sqlConverter.convert(planned, sessionContext);
                try (ConnectorRecordIterator recordIterator = metadata.directQuery(converted, List.of())) {
                    Object[] ignored = recordIterator.next();
                    long duration = System.currentTimeMillis() - start;
                    return ValidationResult.pass(formatRuleWithIdentifier(getRuleName(), modelName.get(), columnName.get()),
                            Duration.succinctDuration(duration, MILLISECONDS));
                }
            }
            catch (Exception e) {
                long duration = System.currentTimeMillis() - start;
                return ValidationResult.fail(formatRuleWithIdentifier(getRuleName(), modelName.get(), columnName.get()), Duration.succinctDuration(duration, MILLISECONDS), e.getMessage());
            }
        }, Executors.newVirtualThreadPerTaskExecutor()));
    }

    private Optional<String> getColumnName(Map<String, Object> parameters)
    {
        return Optional.ofNullable(parameters.get(COLUMN_NAME)).map(Object::toString);
    }

    private Optional<String> getModelName(Map<String, Object> parameters)
    {
        return Optional.ofNullable(parameters.get(MODEL_NAME)).map(Object::toString);
    }
}
