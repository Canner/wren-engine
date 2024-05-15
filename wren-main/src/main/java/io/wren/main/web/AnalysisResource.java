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

package io.wren.main.web;

import com.google.inject.Inject;
import io.trino.sql.tree.Statement;
import io.wren.base.AnalyzedMDL;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionPointAnalyzer;
import io.wren.main.WrenMetastore;
import io.wren.main.web.dto.SqlAnalysisInputDto;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;

import java.util.concurrent.CompletableFuture;

import static io.wren.base.sqlrewrite.Utils.parseSql;
import static io.wren.main.web.WrenExceptionMapper.bindAsyncResponse;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/v1/analysis")
public class AnalysisResource
{
    private final WrenMetastore wrenMetastore;

    @Inject
    public AnalysisResource(
            WrenMetastore wrenMetastore)
    {
        this.wrenMetastore = requireNonNull(wrenMetastore, "wrenMetastore is null");
    }

    @GET
    @Path("/sql")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void getSqlAnalysis(
            SqlAnalysisInputDto inputDto,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(() -> {
                    WrenMDL mdl;
                    if (inputDto.getManifest() == null) {
                        AnalyzedMDL analyzedMDL = wrenMetastore.getAnalyzedMDL();
                        mdl = analyzedMDL.getWrenMDL();
                    }
                    else {
                        mdl = WrenMDL.fromManifest(inputDto.getManifest());
                    }
                    Statement statement = parseSql(inputDto.getSql());
                    return DecisionPointAnalyzer.analyze(
                            statement,
                            SessionContext.builder().setCatalog(mdl.getCatalog()).setSchema(mdl.getSchema()).build(),
                            mdl);
                })
                .whenComplete(bindAsyncResponse(asyncResponse));
    }
}
