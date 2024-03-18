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

import com.google.common.collect.ImmutableList;
import io.accio.base.AccioException;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.client.duckdb.FileUtil;
import io.accio.main.connector.duckdb.DuckDBMetadata;
import io.accio.main.web.dto.QueryResultDto;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.accio.main.web.AccioExceptionMapper.bindAsyncResponse;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/data-source/DuckDB")
public class DuckDBResource
{
    private final DuckDBMetadata metadata;

    @Inject
    public DuckDBResource(
            DuckDBMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @GET
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
            catch (AccioException e) {
                // Sending DDL via executeQuery() still work. Should catch exception to make sense.
                if (e.getMessage().contains("executeQuery() can only be used with queries that return a ResultSet")) {
                    return new QueryResultDto(ImmutableList.of(), ImmutableList.of());
                }
                throw e;
            }
            catch (Exception e) {
                throw new AccioException(GENERIC_USER_ERROR, e);
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
            metadata.setInitSQL(sql);
            metadata.reload();
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
            metadata.setInitSQL(metadata.getInitSQL() + "\n" + sql);
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
            metadata.reload();
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
            metadata.getClient().closeAndInitPool();
            metadata.setSessionSQL(metadata.getSessionSQL() + "\n" + sql);
            FileUtil.appendToFile(metadata.getSessionSQLPath(), sql);
        }).whenComplete(bindAsyncResponse(asyncResponse));
    }
}
