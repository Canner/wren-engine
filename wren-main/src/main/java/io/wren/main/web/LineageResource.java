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

package io.wren.main.web;

import com.google.inject.Inject;
import io.trino.sql.tree.QualifiedName;
import io.wren.base.AnalyzedMDL;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.WrenDataLineage;
import io.wren.main.WrenMetastore;
import io.wren.main.web.dto.ColumnLineageInputDto;
import io.wren.main.web.dto.LineageResult;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.wren.base.Utils.checkArgument;
import static io.wren.base.type.AnyType.ANY;
import static io.wren.main.web.WrenExceptionMapper.bindAsyncResponse;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Path("/v1/lineage")
public class LineageResource
{
    private final WrenMetastore wrenMetastore;

    @Inject
    public LineageResource(
            WrenMetastore wrenMetastore)
    {
        this.wrenMetastore = requireNonNull(wrenMetastore, "wrenMetastore is null");
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
                    WrenDataLineage lineage;
                    WrenMDL mdl;
                    if (inputDto.getManifest() == null) {
                        AnalyzedMDL analyzedMDL = wrenMetastore.getAnalyzedMDL();
                        lineage = analyzedMDL.getWrenDataLineage();
                        mdl = analyzedMDL.getWrenMDL();
                    }
                    else {
                        mdl = WrenMDL.fromManifest(inputDto.getManifest());
                        lineage = WrenDataLineage.analyze(mdl);
                    }
                    checkArgument(inputDto.getModelName() != null && !inputDto.getModelName().isEmpty(),
                            "modelName must be specified");
                    checkArgument(inputDto.getColumnName() != null && !inputDto.getColumnName().isEmpty(),
                            "columnName must be specified");
                    return lineage.getSourceColumns(QualifiedName.of(inputDto.getModelName(), inputDto.getColumnName()))
                            .entrySet()
                            .stream()
                            .map(entry -> new LineageResult(
                                    entry.getKey(),
                                    entry.getValue().stream().map(column ->
                                            new LineageResult.Column(column, Map.of("type", mdl.getColumnType(entry.getKey(), column).orElse(ANY.typName())))).collect(toList())))
                            .collect(Collectors.toList());
                })
                .whenComplete(bindAsyncResponse(asyncResponse));
    }
}
