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

package io.accio.main.web;

import io.accio.base.AccioMDL;
import io.accio.base.dto.Column;
import io.accio.base.dto.CumulativeMetric;
import io.accio.base.sqlrewrite.AccioDataLineage;
import io.accio.main.AccioMetastore;
import io.accio.main.web.dto.ColumnLineageInputDto;
import io.accio.main.web.dto.LineageResult;
import io.accio.main.web.dto.SqlLineageInputDto;
import io.trino.sql.tree.QualifiedName;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.accio.base.Utils.checkArgument;
import static io.accio.main.web.AccioExceptionMapper.bindAsyncResponse;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/lineage")
public class LineageResource
{
    private final AccioMetastore accioMetastore;

    @Inject
    public LineageResource(
            AccioMetastore accioMetastore)
    {
        this.accioMetastore = requireNonNull(accioMetastore, "accioMetastore is null");
    }

    @GET
    @Path("/column")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public void getColumnLineage(
            ColumnLineageInputDto inputDto,
            @Suspended AsyncResponse asyncResponse)
    {
        CompletableFuture
                .supplyAsync(() -> {
                    AccioDataLineage lineage;
                    AccioMDL mdl;
                    if (inputDto.getManifest() == null) {
                        lineage = accioMetastore.getAccioDataLineage();
                        mdl = accioMetastore.getAccioMDL();
                    }
                    else {
                        mdl = AccioMDL.fromManifest(inputDto.getManifest());
                        lineage = AccioDataLineage.analyze(mdl);
                    }
                    checkArgument(inputDto.getModelName() != null && !inputDto.getModelName().isEmpty(),
                            "modelName must be specified");
                    checkArgument(inputDto.getColumnName() != null && !inputDto.getColumnName().isEmpty(),
                            "columnName must be specified");
                    return lineage.getRequiredFields(List.of(QualifiedName.of(inputDto.getModelName(), inputDto.getColumnName())))
                            .entrySet()
                            .stream()
                            .map(entry -> new LineageResult(
                                    entry.getKey(),
                                    entry.getValue().stream().map(column ->
                                            new LineageResult.Column(column, Map.of("type", getColumnType(mdl, entry.getKey(), column)))).collect(toList())))
                            .collect(Collectors.toList());
                })
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    private String getColumnType(AccioMDL mdl, String objectName, String columnName)
    {
        if (!mdl.isObjectExist(objectName)) {
            throw new IllegalArgumentException("Dataset " + objectName + " not found");
        }
        if (mdl.getModel(objectName).isPresent()) {
            return mdl.getModel(objectName).get().getColumns().stream()
                    .filter(column -> columnName.equals(column.getName()))
                    .map(Column::getType)
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("Column " + columnName + " not found in " + objectName));
        }
        else if (mdl.getMetric(objectName).isPresent()) {
            return mdl.getMetric(objectName).get().getColumns().stream()
                    .filter(column -> columnName.equals(column.getName()))
                    .map(Column::getType)
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("Column " + columnName + " not found in " + objectName));
        }
        else if (mdl.getCumulativeMetric(objectName).isPresent()) {
            CumulativeMetric cumulativeMetric = mdl.getCumulativeMetric(objectName).get();
            if (cumulativeMetric.getMeasure().getName().equals(columnName)) {
                return cumulativeMetric.getMeasure().getType();
            }
            if (cumulativeMetric.getWindow().getName().equals(columnName)) {
                return getColumnType(mdl, cumulativeMetric.getBaseObject(), cumulativeMetric.getWindow().getRefColumn());
            }
        }
        throw new IllegalArgumentException("Dataset " + objectName + " is not a model, metric or cumulative metric");
    }

    @GET
    @Path("sql")
    public void getSqlLineage(
            SqlLineageInputDto inputDto,
            @Suspended AsyncResponse asyncResponse)
    {
        // TODO: wait sql lineage implemented
    }
}
