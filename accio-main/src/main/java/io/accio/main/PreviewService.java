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

package io.accio.main;

import com.google.common.collect.Streams;
import io.accio.base.AccioMDL;
import io.accio.base.AnalyzedMDL;
import io.accio.base.SessionContext;
import io.accio.base.sql.SqlConverter;
import io.accio.base.sqlrewrite.AccioPlanner;
import io.accio.main.metadata.Metadata;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PreviewService
{
    private final Metadata metadata;

    private final SqlConverter sqlConverter;

    @Inject
    public PreviewService(
            Metadata metadata,
            SqlConverter sqlConverter)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlConverter = requireNonNull(sqlConverter, "sqlConverter is null");
    }

    public CompletableFuture<List<Object[]>> preview(AccioMDL mdl, String sql, long limit)
    {
        return CompletableFuture.supplyAsync(() -> {
            SessionContext sessionContext = SessionContext.builder()
                    .setCatalog(mdl.getCatalog())
                    .setSchema(mdl.getSchema())
                    .build();

            String planned = AccioPlanner.rewrite(sql, sessionContext, new AnalyzedMDL(mdl, null));
            String converted = sqlConverter.convert(planned, sessionContext);
            return Streams.stream(metadata.directQuery(converted, List.of()))
                    .limit(limit)
                    .collect(toList());
        });
    }
}
