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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.WrenException;
import io.wren.base.client.duckdb.FileUtil;
import io.wren.main.connector.duckdb.DuckDBMetadata;
import io.wren.main.web.dto.QueryResultDto;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;

import static io.wren.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.wren.main.web.WrenExceptionMapper.bindAsyncResponse;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

@Path("/v1/data-source/duckdb")
public class DuckDBResource
{
    private final DuckDBMetadata metadata;

    @Inject
    public DuckDBResource(
            DuckDBMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @POST
    @Path("/query")
    @Produces(APPLICATION_JSON)
    public void query(
            String statement,
            @Suspended AsyncResponse asyncResponse)
            throws Exception
    {
        supplyAsync(() -> {
            try (ConnectorRecordIterator iterator = metadata.directQuery(statement, ImmutableList.of())) {
                ImmutableList.Builder<Object[]> data = ImmutableList.builder();
                while (iterator.hasNext()) {
                    data.add(iterator.next());
                }
                return new QueryResultDto(iterator.getColumns(), data.build());
            }
            catch (WrenException e) {
                // Sending DDL via executeQuery() still work. Should catch exception to make sense.
                if (e.getMessage().contains("executeQuery() can only be used with queries that return a ResultSet")) {
                    return new QueryResultDto(ImmutableList.of(), ImmutableList.of());
                }
                throw e;
            }
            catch (Exception e) {
                throw new WrenException(GENERIC_USER_ERROR, e);
            }
        }).whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/settings/init-sql")
    public void getInitSQL(@Suspended AsyncResponse asyncResponse)
    {
        supplyAsync(metadata::getInitSQL)
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @PUT
    @Path("/settings/init-sql")
    public void setInitSQL(
            String sql,
            @Suspended AsyncResponse asyncResponse)
    {
        runAsync(() -> {
            String initSQL = metadata.getInitSQL();
            metadata.setInitSQL(sql);
            try {
                metadata.reload();
            }
            catch (Exception e) {
                metadata.setInitSQL(initSQL);
                throw e;
            }
            java.nio.file.Path initSQLPath = metadata.getInitSQLPath();
            FileUtil.archiveFile(initSQLPath);
            FileUtil.createFile(initSQLPath, sql);
        }).whenComplete(bindAsyncResponse(asyncResponse));
    }

    @PATCH
    @Path("/settings/init-sql")
    public void appendToInitSQL(
            String sql,
            @Suspended AsyncResponse asyncResponse)
    {
        runAsync(() -> {
            metadata.directDDL(sql);
            metadata.appendInitSQL(sql);
            FileUtil.appendToFile(metadata.getInitSQLPath(), sql);
        }).whenComplete(bindAsyncResponse(asyncResponse));
    }

    @GET
    @Path("/settings/session-sql")
    public void getSessionSQL(@Suspended AsyncResponse asyncResponse)
    {
        supplyAsync(metadata::getSessionSQL)
                .whenComplete(bindAsyncResponse(asyncResponse));
    }

    @PUT
    @Path("/settings/session-sql")
    public void setSessionSQL(
            String sql,
            @Suspended AsyncResponse asyncResponse)
    {
        runAsync(() -> {
            metadata.setSessionSQL(sql);
            metadata.getClient().closeAndInitPool();
            java.nio.file.Path sessionSQLPath = metadata.getSessionSQLPath();
            FileUtil.archiveFile(sessionSQLPath);
            FileUtil.createFile(sessionSQLPath, sql);
        }).whenComplete(bindAsyncResponse(asyncResponse));
    }

    @PATCH
    @Path("/settings/session-sql")
    public void appendToSessionSQL(
            String sql,
            @Suspended AsyncResponse asyncResponse)
    {
        runAsync(() -> {
            String sessionSQL = metadata.getSessionSQL();
            metadata.appendSessionSQL(sql);
            try {
                metadata.getClient().closeAndInitPool();
            }
            catch (Exception e) {
                metadata.setSessionSQL(sessionSQL);
                metadata.getClient().initPool();
                throw e;
            }
            FileUtil.appendToFile(metadata.getSessionSQLPath(), sql);
        }).whenComplete(bindAsyncResponse(asyncResponse));
    }
}
