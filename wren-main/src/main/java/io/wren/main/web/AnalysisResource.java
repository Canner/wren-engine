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
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Statement;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.DecisionPointAnalyzer;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.ExprSource;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.FilterAnalysis;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.QueryAnalysis;
import io.wren.base.sqlrewrite.analyzer.decisionpoint.RelationAnalysis;
import io.wren.main.web.dto.NodeLocationDto;
import io.wren.main.web.dto.QueryAnalysisDto;
import io.wren.main.web.dto.QueryAnalysisDto.ColumnAnalysisDto;
import io.wren.main.web.dto.QueryAnalysisDto.FilterAnalysisDto;
import io.wren.main.web.dto.QueryAnalysisDto.RelationAnalysisDto;
import io.wren.main.web.dto.QueryAnalysisDto.SortItemAnalysisDto;
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

@Deprecated
@Path("/v1/analysis")
public class AnalysisResource
{
    @Inject
    public AnalysisResource()
    {
    }

    @GET
    @Path("/sql")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void getSqlAnalysis(
            SqlAnalysisInputDto inputDto,
            @Suspended AsyncResponse asyncResponse)
    {
        if (inputDto.getManifest() == null) {
            asyncResponse.resume(new IllegalArgumentException("Manifest is required"));
        }
        CompletableFuture
            .supplyAsync(() -> {
                WrenMDL mdl = WrenMDL.fromManifest(inputDto.getManifest());
                Statement statement = parseSql(inputDto.getSql());
                return DecisionPointAnalyzer.analyze(
                        statement,
                        SessionContext.builder().setCatalog(mdl.getCatalog()).setSchema(mdl.getSchema()).build(),
                        mdl).stream().map(AnalysisResource::toQueryAnalysisDto).toList();
            }).whenComplete(bindAsyncResponse(asyncResponse));
    }

    static QueryAnalysisDto toQueryAnalysisDto(QueryAnalysis queryAnalysis)
    {
        return new QueryAnalysisDto(
                queryAnalysis.getSelectItems().stream().map(AnalysisResource::toColumnAnalysisDto).toList(),
                toRelationAnalysisDto(queryAnalysis.getRelation()),
                toFilterAnalysisDto(queryAnalysis.getFilter()),
                queryAnalysis.getGroupByKeys().stream().map(groupByKeys -> groupByKeys.stream().map(AnalysisResource::toGroupByKeyDto).toList()).toList(),
                queryAnalysis.getSortings().stream().map(AnalysisResource::toSortItemAnalysisDto).toList(),
                queryAnalysis.isSubqueryOrCte());
    }

    private static ColumnAnalysisDto toColumnAnalysisDto(QueryAnalysis.ColumnAnalysis columnAnalysis)
    {
        return new ColumnAnalysisDto(columnAnalysis.getAliasName(),
                columnAnalysis.getExpression(), columnAnalysis.getProperties(),
                toNodeLocationDto(columnAnalysis.getNodeLocation()),
                columnAnalysis.getExprSources().stream().map(AnalysisResource::toExprSourceDto).toList());
    }

    private static FilterAnalysisDto toFilterAnalysisDto(FilterAnalysis filterAnalysis)
    {
        return switch (filterAnalysis) {
            case FilterAnalysis.ExpressionAnalysis exprAnalysis -> new FilterAnalysisDto(
                    exprAnalysis.getType().name(),
                    null,
                    null,
                    exprAnalysis.getNode(),
                    toNodeLocationDto(exprAnalysis.getNodeLocation()),
                    exprAnalysis.getExprSources().stream().map(AnalysisResource::toExprSourceDto).toList());
            case FilterAnalysis.LogicalAnalysis logicalAnalysis ->
                    new FilterAnalysisDto(
                            logicalAnalysis.getType().name(),
                            toFilterAnalysisDto(logicalAnalysis.getLeft()),
                            toFilterAnalysisDto(logicalAnalysis.getRight()),
                            null,
                            toNodeLocationDto(logicalAnalysis.getNodeLocation()),
                            null);
            case null -> null;
            default -> throw new IllegalArgumentException("Unsupported filter analysis: " + filterAnalysis);
        };
    }

    private static RelationAnalysisDto toRelationAnalysisDto(RelationAnalysis relationAnalysis)
    {
        return switch (relationAnalysis) {
            case RelationAnalysis.TableRelation tableRelation ->
                    new RelationAnalysisDto(tableRelation.getType().name(),
                            tableRelation.getAlias(),
                            null,
                            null,
                            null,
                            tableRelation.getTableName(),
                            null,
                            null,
                            toNodeLocationDto(tableRelation.getNodeLocation()));
            case RelationAnalysis.JoinRelation joinRelation -> new RelationAnalysisDto(
                    joinRelation.getType().name(),
                    joinRelation.getAlias(),
                    toRelationAnalysisDto(joinRelation.getLeft()),
                    toRelationAnalysisDto(joinRelation.getRight()),
                    joinCriteriaDto(joinRelation.getCriteria()),
                    null,
                    null,
                    joinRelation.getExprSources().stream().map(AnalysisResource::toExprSourceDto).toList(),
                    toNodeLocationDto(joinRelation.getNodeLocation()));
            case RelationAnalysis.SubqueryRelation subqueryRelation -> new RelationAnalysisDto(
                    subqueryRelation.getType().name(),
                    subqueryRelation.getAlias(),
                    null,
                    null,
                    null,
                    null,
                    subqueryRelation.getBody().stream().map(AnalysisResource::toQueryAnalysisDto).toList(),
                    null,
                    toNodeLocationDto(subqueryRelation.getNodeLocation()));
            case null -> null;
            default -> throw new IllegalArgumentException("Unsupported relation analysis: " + relationAnalysis);
        };
    }

    private static SortItemAnalysisDto toSortItemAnalysisDto(QueryAnalysis.SortItemAnalysis sortItemAnalysis)
    {
        return new SortItemAnalysisDto(sortItemAnalysis.getExpression(),
                sortItemAnalysis.getOrdering().name(),
                toNodeLocationDto(sortItemAnalysis.getNodeLocation()),
                sortItemAnalysis.getExprSources().stream().map(AnalysisResource::toExprSourceDto).toList());
    }

    private static QueryAnalysisDto.ExprSourceDto toExprSourceDto(ExprSource exprSource)
    {
        return new QueryAnalysisDto.ExprSourceDto(exprSource.expression(), exprSource.sourceDataset(), exprSource.sourceColumn(), toNodeLocationDto(exprSource.nodeLocation()));
    }

    private static QueryAnalysisDto.GroupByKeyDto toGroupByKeyDto(QueryAnalysis.GroupByKey groupByKey)
    {
        return new QueryAnalysisDto.GroupByKeyDto(groupByKey.getExpression(),
                toNodeLocationDto(groupByKey.getNodeLocation()),
                groupByKey.getExprSources().stream().map(AnalysisResource::toExprSourceDto).toList());
    }

    private static QueryAnalysisDto.JoinCriteriaDto joinCriteriaDto(RelationAnalysis.JoinCriteria joinCriteria)
    {
        return new QueryAnalysisDto.JoinCriteriaDto(joinCriteria.getExpression(), toNodeLocationDto(joinCriteria.getNodeLocation()));
    }

    private static NodeLocationDto toNodeLocationDto(NodeLocation nodeLocation)
    {
        return new NodeLocationDto(nodeLocation.getLineNumber(), nodeLocation.getColumnNumber());
    }
}
