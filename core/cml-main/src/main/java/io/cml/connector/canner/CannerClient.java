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

package io.cml.connector.canner;

import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.cml.connector.canner.dto.TableDto;
import io.cml.connector.canner.dto.TrinoQueryResult;
import io.cml.connector.canner.dto.WorkspaceDto;
import io.cml.connector.jdbc.JdbcType;
import io.cml.spi.CmlException;
import io.cml.spi.Column;
import io.cml.spi.ConnectorRecordIterator;
import io.cml.spi.Parameter;
import io.cml.spi.type.PGType;
import io.cml.web.dto.ErrorMessageDto;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.cml.Utils.randomIntString;
import static io.cml.Utils.randomTableSuffix;
import static io.cml.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class CannerClient
{
    private static final String X_CANNERFLOW_QUERY_CACHE_TTL = "X-Cannerflow-Query-Cache-Ttl";
    public static final String X_CANNERFLOW_QUERY_CACHE_REFRESH = "X-Cannerflow-Query-Cache-Refresh";
    public static final String X_CANNERFLOW_QUERY_WORKSPACES = "X-Cannerflow-Query-Workspaces";
    private static final String X_TRINO_USER = "X-Trino-User";
    private static final String X_TRINO_PREPARED_STATEMENT = "X-Trino-Prepared-Statement";

    private static final String CANNER_USER = "canner";

    private static final JsonCodec<ErrorMessageDto> ERROR_MESSAGE_DTO_JSON_CODEC = jsonCodec(ErrorMessageDto.class);
    private static final JsonCodec<WorkspaceDto> WORKSPACE_DTO_JSON_CODEC = jsonCodec(WorkspaceDto.class);
    private static final JsonCodec<List<WorkspaceDto>> LIST_WORKSPACE_DTO_JSON_CODEC = listJsonCodec(WorkspaceDto.class);
    private static final JsonCodec<List<TableDto>> LIST_TABLE_DTO_JSON_CODEC = listJsonCodec(TableDto.class);
    private static final JsonCodec<TrinoQueryResult> TRINO_QUERY_RESULT_JSON_CODEC = jsonCodec(TrinoQueryResult.class);

    private final String personalAccessToken;
    private final URI cannerUrl;
    private final String pgUrl;
    private final String keycloakGroupId;
    private final String cannerAvailableWorkspace;

    private final HttpClient httpClient;

    public CannerClient(CannerConfig cannerConfig)
    {
        this.personalAccessToken = requireNonNull(cannerConfig.getPersonalAccessToken(), "personalAccessToken is null");
        this.cannerUrl = URI.create(requireNonNull((cannerConfig.getCannerUrl()), "cannerUrl is null") + ":" + cannerConfig.getTrinoPort());
        this.pgUrl = cannerUrl.getHost() + ":" + cannerConfig.getPgPort();
        this.cannerAvailableWorkspace = requireNonNull(cannerConfig.getAvailableWorkspace(), "cannerAvailableWorkspace is null");
        // TODO: how to generate keycloak group id?
        this.keycloakGroupId = UUID.randomUUID().toString();
        this.httpClient = new JettyHttpClient(new HttpClientConfig().setIdleTimeout(new Duration(20, SECONDS)));
    }

    public List<WorkspaceDto> listWorkspaces()
    {
        Request request = prepareGet()
                .setUri(cannerUrl.resolve("/v1/catalog/workspace"))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }

        return LIST_WORKSPACE_DTO_JSON_CODEC.fromJson(response.getBody());
    }

    public Optional<WorkspaceDto> getOneWorkspace(String workspaceId)
    {
        Request request = prepareGet()
                .setUri(cannerUrl.resolve("/v1/catalog/workspace/" + workspaceId))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        switch (response.getStatusCode()) {
            case 200:
                return java.util.Optional.of(WORKSPACE_DTO_JSON_CODEC.fromJson(response.getBody()));
            case 404:
                return Optional.empty();
            default:
                throw getWebApplicationException(response);
        }
    }

    public Optional<WorkspaceDto> getWorkspaceBySqlName(String sqlName)
    {
        return listWorkspaces().stream().filter(workspaceDto -> workspaceDto.getSqlName().equals(sqlName)).findAny();
    }

    public void createWorkspace(String name)
    {
        WorkspaceDto workspaceDto = new WorkspaceDto(name, name + randomTableSuffix(), keycloakGroupId);

        Request request = preparePost()
                .setUri(cannerUrl.resolve("/v1/catalog/workspace/" + name))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(WORKSPACE_DTO_JSON_CODEC, workspaceDto))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }
    }

    public List<TableDto> listTables(String workspaceId)
    {
        Request request = prepareGet()
                .setUri(cannerUrl.resolve("/v1/catalog/workspace/" + workspaceId + "/table"))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }

        return LIST_TABLE_DTO_JSON_CODEC.fromJson(response.getBody());
    }

    public ConnectorRecordIterator query(String sql, List<Parameter> parameters)
    {
        try (Connection connection = createConnection()) {
            PreparedStatement statement = connection.prepareStatement(sql);
            for (int i = 0; i < parameters.size(); i++) {
                statement.setObject(i + 1, parameters.get(i).getValue(), JdbcType.toJdbcType(parameters.get(i).getType()));
            }
            statement.execute();
            ResultSet resultSet = statement.getResultSet();
            return new JdbcRecordIterator(resultSet);
        }
        catch (SQLException se) {
            throw new CmlException(GENERIC_INTERNAL_ERROR, se);
        }
    }

    public TrinoRecordIterator describe(String sql, List<Parameter> parameters)
    {
        String statementName = "statement_" + randomIntString();

        Request request = preparePost()
                .setHeader(X_TRINO_USER, CANNER_USER)
                .setHeader(X_CANNERFLOW_QUERY_CACHE_REFRESH, "false")
                .setHeader(X_CANNERFLOW_QUERY_CACHE_TTL, "300")
                .setHeader(X_CANNERFLOW_QUERY_WORKSPACES, cannerAvailableWorkspace)
                .setHeader(X_TRINO_PREPARED_STATEMENT, toTrinoPreparedStatement(statementName, sql))
                .setUri(cannerUrl.resolve("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator(format("DESCRIBE OUTPUT %s", statementName), UTF_8))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }

        return new TrinoRecordIterator(TRINO_QUERY_RESULT_JSON_CODEC.fromJson(response.getBody()));
    }

    private static String toTrinoPreparedStatement(String name, String sql)
    {
        return format("%s=%s", URLEncoder.encode(name, UTF_8), URLEncoder.encode(sql, UTF_8));
    }

    public TrinoQueryResult getNextUri(URI uri)
    {
        Request request = prepareGet()
                .setUri(uri)
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }

        return TRINO_QUERY_RESULT_JSON_CODEC.fromJson(response.getBody());
    }

    public <T, E extends Exception> T executeHttpRequest(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        return httpClient.execute(request, responseHandler);
    }

    public static WebApplicationException getWebApplicationException(StringResponseHandler.StringResponse response)
    {
        String body = response.getBody();
        ErrorMessageDto errorMessageDto;
        try {
            errorMessageDto = ERROR_MESSAGE_DTO_JSON_CODEC.fromJson(body);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(format("Illegal response body '%s' with status code %d", body, response.getStatusCode()), e);
        }

        throw new WebApplicationException(
                Response.status(response.getStatusCode())
                        .type(APPLICATION_JSON)
                        .entity(errorMessageDto)
                        .build());
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:postgresql://%s/%s", pgUrl, cannerAvailableWorkspace);
        Properties props = getDefaultProperties();
        return DriverManager.getConnection(url, props);
    }

    private Properties getDefaultProperties()
    {
        Properties props = new Properties();
        props.setProperty("password", personalAccessToken);
        props.setProperty("user", "canner");
        props.setProperty("ssl", "false");
        return props;
    }

    public class JdbcRecordIterator
            implements ConnectorRecordIterator
    {
        private final List<Column> columns;
        private int columnCount;

        private final ResultSet resultSet;

        private boolean hasNext;

        private Object[] nowBuffer;

        public JdbcRecordIterator(ResultSet resultSet)
                throws SQLException
        {
            this.resultSet = resultSet;
            this.columns = buildColumns();

            hasNext = resultSet.next();
            if (hasNext) {
                nowBuffer = getCurrentRecord();
            }
        }

        @Override
        public List<PGType> getTypes()
        {
            return columns.stream().map(Column::getType).collect(toList());
        }

        @Override
        public void close()
                throws IOException
        {
        }

        @Override
        public boolean hasNext()
        {
            return hasNext;
        }

        @Override
        public Object[] next()
        {
            Object[] nowRecord = nowBuffer;
            try {
                hasNext = resultSet.next();
                if (hasNext) {
                    nowBuffer = getCurrentRecord();
                }
            }
            catch (SQLException e) {
                throw new CmlException(GENERIC_INTERNAL_ERROR, e);
            }
            return nowRecord;
        }

        private List<Column> buildColumns()
                throws SQLException
        {
            ImmutableList.Builder<Column> builder = ImmutableList.builder();
            ResultSetMetaData metaData = resultSet.getMetaData();
            this.columnCount = metaData.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                builder.add(new Column(metaData.getColumnName(i), JdbcType.toPGType(metaData.getColumnType(i))));
            }
            return builder.build();
        }

        private Object[] getCurrentRecord()
                throws SQLException
        {
            ImmutableList.Builder builder = ImmutableList.builder();
            for (int i = 1; i <= columnCount; i++) {
                builder.add(resultSet.getObject(i));
            }
            return builder.build().toArray();
        }
    }

    public class TrinoRecordIterator
            implements ConnectorRecordIterator
    {
        private final List<Column> columns;
        private TrinoQueryResult buffer;

        private Iterator<List<Object>> dataIter;

        public TrinoRecordIterator(TrinoQueryResult queryResult)
        {
            this.buffer = waitForData(queryResult);
            this.columns = buffer.getColumns().stream()
                    .map(column -> new Column((String) column.get("name"), CannerType.toPGType((String) column.get("type"))))
                    .collect(toList());
            this.dataIter = buffer.getData().iterator();
        }

        private TrinoQueryResult waitForData(TrinoQueryResult queryResultDto)
        {
            while (queryResultDto.getData() == null) {
                queryResultDto = getNextUri(queryResultDto.getNextUri());
            }
            return queryResultDto;
        }

        @Override
        public List<PGType> getTypes()
        {
            return columns.stream().map(Column::getType).collect(toList());
        }

        @Override
        public void close()
                throws IOException
        {
        }

        @Override
        public boolean hasNext()
        {
            return !buffer.getStats().get("state").equals("FINISHED") &&
                    !buffer.getStats().get("state").equals("FAILED") &&
                    dataIter.hasNext();
        }

        @Override
        public Object[] next()
        {
            if (!dataIter.hasNext()) {
                buffer = waitForData(getNextUri(buffer.getNextUri()));
                dataIter = buffer.getData().iterator();
            }
            return dataIter.next().toArray();
        }
    }
}
