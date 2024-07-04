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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.wren.main;

import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.wren.base.AnalyzedMDL;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.config.ConfigManager;
import io.wren.base.config.WrenConfig;
import io.wren.base.sql.SqlConverter;
import io.wren.base.sqlrewrite.WrenPlanner;
import io.wren.main.metadata.Metadata;
import io.wren.main.web.dto.QueryResultDto;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PreviewService
{
    private static final Logger LOG = Logger.get(PreviewService.class);
    private final Metadata metadata;

    private final SqlConverter sqlConverter;
    private final ConfigManager configManager;

    @Inject
    public PreviewService(
            Metadata metadata,
            SqlConverter sqlConverter,
            ConfigManager configManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
        this.configManager = requireNonNull(configManager, "configManager is null");
    }

    public CompletableFuture<QueryResultDto> preview(WrenMDL mdl, String sql, long limit)
    {
        return CompletableFuture.supplyAsync(() -> {
            WrenConfig config = configManager.getConfig(WrenConfig.class);
            SessionContext sessionContext = SessionContext.builder()
                    .setCatalog(mdl.getCatalog())
                    .setSchema(mdl.getSchema())
                    .setEnableDynamic(config.getEnableDynamicFields())
                    .build();

            String planned = WrenPlanner.rewrite(sql, sessionContext, new AnalyzedMDL(mdl, null));
            String converted = sqlConverter.convert(planned, sessionContext);
            try (ConnectorRecordIterator iter = metadata.directQuery(converted, List.of())) {
                return new QueryResultDto(
                        iter.getColumns(),
                        Streams.stream(iter).limit(limit).collect(toList()));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public CompletableFuture<String> dryPlan(WrenMDL mdl, String sql, boolean isModelingOnly)
    {
        return CompletableFuture.supplyAsync(() -> {
            WrenConfig config = configManager.getConfig(WrenConfig.class);
            SessionContext sessionContext = SessionContext.builder()
                    .setCatalog(mdl.getCatalog())
                    .setSchema(mdl.getSchema())
                    .setEnableDynamic(config.getEnableDynamicFields())
                    .build();

            String planned = WrenPlanner.rewrite(sql, sessionContext, new AnalyzedMDL(mdl, null));
            if (isModelingOnly) {
                LOG.info("Planned SQL: %s", planned);
                return planned;
            }
            return sqlConverter.convert(planned, sessionContext);
        });
    }

    public CompletableFuture<List<Column>> dryRun(WrenMDL mdl, String sql)
    {
        return CompletableFuture.supplyAsync(() -> {
            WrenConfig config = configManager.getConfig(WrenConfig.class);
            SessionContext sessionContext = SessionContext.builder()
                    .setCatalog(mdl.getCatalog())
                    .setSchema(mdl.getSchema())
                    .setEnableDynamic(config.getEnableDynamicFields())
                    .build();

            String planned = WrenPlanner.rewrite(sql, sessionContext, new AnalyzedMDL(mdl, null));
            String converted = sqlConverter.convert(planned, sessionContext);
            return metadata.describeQuery(converted, List.of());
        });
    }
}
