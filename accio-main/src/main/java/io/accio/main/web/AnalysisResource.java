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

package io.accio.main.web;

import io.accio.base.AccioMDL;
import io.accio.base.AnalyzedMDL;
import io.accio.base.SessionContext;
import io.accio.base.sqlrewrite.analyzer.Analysis;
import io.accio.base.sqlrewrite.analyzer.StatementAnalyzer;
import io.accio.main.AccioMetastore;
import io.accio.main.web.dto.PredicateDto;
import io.accio.main.web.dto.SqlAnalysisInputDto;
import io.accio.main.web.dto.SqlAnalysisOutputDto;
import io.trino.sql.tree.QualifiedName;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.sqlrewrite.Utils.parseSql;
import static io.accio.base.sqlrewrite.analyzer.Analysis.SimplePredicate;
import static io.accio.main.web.AccioExceptionMapper.bindAsyncResponse;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/analysis")
public class AnalysisResource
{
    private final AccioMetastore accioMetastore;

    @Inject
    public AnalysisResource(
            AccioMetastore accioMetastore)
    {
        this.accioMetastore = requireNonNull(accioMetastore, "accioMetastore is null");
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
                    AccioMDL mdl;
                    if (inputDto.getManifest() == null) {
                        AnalyzedMDL analyzedMDL = accioMetastore.getAnalyzedMDL();
                        mdl = analyzedMDL.getAccioMDL();
                    }
                    else {
                        mdl = AccioMDL.fromManifest(inputDto.getManifest());
                    }
                    Analysis analysis = StatementAnalyzer.analyze(
                            parseSql(inputDto.getSql()),
                            SessionContext.builder().setCatalog(mdl.getCatalog()).setSchema(mdl.getSchema()).build(),
                            mdl);
                    return toSqlAnalysisOutputDto(analysis.getSimplePredicates());
                })
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    private static List<SqlAnalysisOutputDto> toSqlAnalysisOutputDto(List<SimplePredicate> predicates)
    {
        return predicates.stream()
                .collect(groupingBy(predicate -> QualifiedName.of(
                        predicate.getTableName().getSchemaTableName().getTableName(),
                        predicate.getColumnName())))
                .entrySet()
                .stream()
                .map(e ->
                        new SqlAnalysisOutputDto(
                                e.getKey().getParts().get(0),
                                e.getKey().getParts().get(1),
                                e.getValue().stream()
                                        .map(simplePredicate -> new PredicateDto(simplePredicate.getOperator(), simplePredicate.getValue()))
                                        .collect(toImmutableList())))
                .collect(toImmutableList());
    }
}
