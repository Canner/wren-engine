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

import io.trino.sql.ExpressionFormatter;
import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Statement;
import io.wren.base.AnalyzedMDL;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.analyzer.Analysis;
import io.wren.base.sqlrewrite.analyzer.StatementAnalyzer;
import io.wren.main.WrenMetastore;
import io.wren.main.web.dto.ColumnPredicateDto;
import io.wren.main.web.dto.PredicateDto;
import io.wren.main.web.dto.SqlAnalysisInputDto;
import io.wren.main.web.dto.SqlAnalysisOutputDto;

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
import static io.wren.base.sqlrewrite.Utils.parseSql;
import static io.wren.base.sqlrewrite.analyzer.Analysis.SimplePredicate;
import static io.wren.main.web.WrenExceptionMapper.bindAsyncResponse;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

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
                    Analysis analysis = new Analysis(statement);
                    StatementAnalyzer.analyze(
                            analysis,
                            statement,
                            SessionContext.builder().setCatalog(mdl.getCatalog()).setSchema(mdl.getSchema()).build(),
                            mdl);
                    return toSqlAnalysisOutputDto(analysis);
                })
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    private static List<SqlAnalysisOutputDto> toSqlAnalysisOutputDto(Analysis analysis)
    {
        return analysis.getSimplePredicates().stream()
                .collect(
                        // group by table name
                        groupingBy(predicate -> predicate.getTableName().getSchemaTableName().getTableName(),
                                // then group by column name
                                groupingBy(SimplePredicate::getColumnName)))
                .entrySet()
                .stream()
                .map(e ->
                        new SqlAnalysisOutputDto(
                                e.getKey(),
                                e.getValue().entrySet().stream()
                                        .map(columns ->
                                                new ColumnPredicateDto(
                                                        columns.getKey(),
                                                        columns.getValue().stream()
                                                                .map(predicate -> new PredicateDto(predicate.getOperator(), predicate.getValue()))
                                                                .collect(toImmutableList())))
                                        .collect(toImmutableList()),
                                analysis.getLimit().map(AnalysisResource::formatExpression).orElse(null),
                                analysis.getSortItems().stream()
                                        .map(item -> new SqlAnalysisOutputDto.SortItem(item.getSortKey().toString(), item.getOrdering())).collect(toImmutableList())))
                .collect(toImmutableList());
    }

    private static String formatExpression(Expression expression)
    {
        return ExpressionFormatter.formatExpression(expression, SqlFormatter.Dialect.DEFAULT);
    }
}
