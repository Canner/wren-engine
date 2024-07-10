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
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionPointAnalyzer;
import io.wren.main.web.dto.SqlAnalysisInputBatchDto;
import io.wren.main.web.dto.SqlAnalysisInputDtoV2;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.wren.base.sqlrewrite.Utils.parseSql;
import static io.wren.main.web.WrenExceptionMapper.bindAsyncResponse;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.nio.charset.StandardCharsets.UTF_8;

@Path("/v2/analysis")
public class AnalysisResourceV2
{
    @Inject
    public AnalysisResourceV2() {}

    @GET
    @Path("/sql")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void getSqlAnalysis(
            SqlAnalysisInputDtoV2 inputDto,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(() ->
                        Optional.ofNullable(inputDto.getManifestStr())
                                .orElseThrow(() -> new IllegalArgumentException("Manifest is required")))
                .thenApply(manifestStr -> {
                    try {
                        return WrenMDL.fromJson(new String(Base64.getDecoder().decode(manifestStr), UTF_8));
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .thenApply(mdl -> {
                    Statement statement = parseSql(inputDto.getSql());
                    return DecisionPointAnalyzer.analyze(
                            statement,
                            SessionContext.builder().setCatalog(mdl.getCatalog()).setSchema(mdl.getSchema()).build(),
                            mdl).stream().map(AnalysisResource::toQueryAnalysisDto).toList();
                })
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/sqls")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void getSqlAnalysisBatch(
            SqlAnalysisInputBatchDto inputBatchDto,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(() ->
                        Optional.ofNullable(inputBatchDto.getManifestStr())
                                .orElseThrow(() -> new IllegalArgumentException("Manifest is required")))
                .thenApply(manifestStr -> {
                    try {
                        return WrenMDL.fromJson(new String(Base64.getDecoder().decode(manifestStr), UTF_8));
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .thenApply(mdl ->
                  inputBatchDto.getSqls().stream().map(sql -> {
                      Statement statement = parseSql(sql);
                      return DecisionPointAnalyzer.analyze(
                            statement,
                            SessionContext.builder().setCatalog(mdl.getCatalog()).setSchema(mdl.getSchema()).build(),
                            mdl).stream().map(AnalysisResource::toQueryAnalysisDto).toList();
                  }).toList())
                .whenComplete(bindAsyncResponse(asyncResponse));
    }
}
