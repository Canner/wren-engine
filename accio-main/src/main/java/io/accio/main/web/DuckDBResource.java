package io.accio.main.web;

import com.google.common.collect.ImmutableList;
import io.accio.base.Column;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.client.duckdb.FileUtil;
import io.accio.main.connector.duckdb.DuckDBMetadata;
import io.accio.main.web.dto.DuckDBQueryDto;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/v1/data-source/DuckDB")
public class DuckDBResource
        extends DataSourceResource
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
        try (ConnectorRecordIterator iterator = metadata.directQuery(statement, ImmutableList.of())) {
            ImmutableList.Builder<List<Object>> rowsBuilder = ImmutableList.builder();
            while (iterator.hasNext()) {
                rowsBuilder.add(Arrays.asList(iterator.next()));
            }
            DuckDBQueryDto dto = DuckDBQueryDto.builder()
                    .columns(iterator.getColumns().stream().map(this::toDtoColumn).collect(toImmutableList()))
                    .rows(rowsBuilder.build())
                    .build();
            asyncResponse.resume(Response.ok(dto).build());
        }
    }

    @GET
    @Path("/settings/init-sql")
    public String getInitSQL()
    {
        return metadata.getInitSQL();
    }

    @PUT
    @Path("/settings/init-sql")
    public void setInitSQL(
            String sql,
            @Suspended AsyncResponse asyncResponse)
    {
        metadata.setInitSQL(sql);
        metadata.reload();
        java.nio.file.Path initSQLPath = metadata.getInitSQLPath();
        FileUtil.archiveFile(initSQLPath);
        FileUtil.createFile(initSQLPath, sql);
        asyncResponse.resume(Response.ok().build());
    }

    @PATCH
    @Path("/settings/init-sql")
    public void appendToInitSQL(
            String sql,
            @Suspended AsyncResponse asyncResponse)
    {
        metadata.directDDL(sql);
        metadata.setInitSQL(metadata.getInitSQL() + "\n" + sql);
        FileUtil.appendToFile(metadata.getInitSQLPath(), sql);
        asyncResponse.resume(Response.ok().build());
    }

    @GET
    @Path("/settings/session-sql")
    public String getSessionSQL()
    {
        return metadata.getSessionSQL();
    }

    @PUT
    @Path("/settings/session-sql")
    public void setSessionSQL(
            String sql,
            @Suspended AsyncResponse asyncResponse)
    {
        metadata.setSessionSQL(sql);
        metadata.reload();
        java.nio.file.Path sessionSQLPath = metadata.getSessionSQLPath();
        FileUtil.archiveFile(sessionSQLPath);
        FileUtil.createFile(sessionSQLPath, sql);
        asyncResponse.resume(Response.ok().build());
    }

    @PATCH
    @Path("/settings/session-sql")
    public void appendToSessionSQL(
            String sql,
            @Suspended AsyncResponse asyncResponse)
    {
        metadata.directDDL(sql);
        metadata.setSessionSQL(metadata.getSessionSQL() + "\n" + sql);
        FileUtil.appendToFile(metadata.getSessionSQLPath(), sql);
        asyncResponse.resume(Response.ok().build());
    }

    private DuckDBQueryDto.Column toDtoColumn(Column column)
    {
        return DuckDBQueryDto.Column.of(column.getName(), column.getType().typName());
    }
}
