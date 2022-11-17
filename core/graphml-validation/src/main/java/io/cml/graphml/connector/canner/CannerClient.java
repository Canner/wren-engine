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

package io.cml.graphml.connector.canner;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.cml.graphml.connector.Client;
import io.cml.graphml.connector.ColumnDescription;
import io.cml.graphml.connector.jdbc.JdbcRecordIterator;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.cml.graphml.Utils.randomIntString;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public final class CannerClient
        implements Client
{
    private static final String X_CANNERFLOW_QUERY_CACHE_TTL = "X-Cannerflow-Query-Cache-Ttl";
    public static final String X_CANNERFLOW_QUERY_CACHE_REFRESH = "X-Cannerflow-Query-Cache-Refresh";
    public static final String X_CANNERFLOW_QUERY_WORKSPACES = "X-Cannerflow-Query-Workspaces";
    private static final String X_TRINO_USER = "X-Trino-User";
    private static final String X_TRINO_PREPARED_STATEMENT = "X-Trino-Prepared-Statement";

    private static final String CANNER_USER = "canner";

    private static final JsonCodec<ErrorMessageDto> ERROR_MESSAGE_DTO_JSON_CODEC = jsonCodec(ErrorMessageDto.class);
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

    @Override
    public Iterator<Object[]> query(String sql)
    {
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            return new JdbcRecordIterator(resultSet);
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    /**
     * We invoke Trino sync API to describe the specific sql because describe doesn't
     * be supported by Canner PG wire protocol.
     */
    @Override
    public Iterator<ColumnDescription> describe(String sql)
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

    @Override
    public void executeDDL(String sql)
    {
        throw new UnsupportedOperationException("Canner client doesn't supports to execute ddl");
    }

    @Override
    public List<String> listTables()
    {
        try (Connection connection = createConnection()) {
            ResultSet resultSet = connection.getMetaData().getTables("canner", cannerAvailableWorkspace, null, null);
            List<String> names = new ArrayList<>();
            while (resultSet.next()) {
                String tableName = resultSet.getString(3);
                names.add(tableName);
            }
            return names;
        }
        catch (SQLException se) {
            throw new RuntimeException(se);
        }
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

    public class TrinoRecordIterator
            implements Iterator<ColumnDescription>
    {
        private TrinoQueryResult buffer;

        private Iterator<List<Object>> dataIter;

        public TrinoRecordIterator(TrinoQueryResult queryResult)
        {
            this.buffer = waitForData(queryResult);
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
        public boolean hasNext()
        {
            return !buffer.getStats().get("state").equals("FINISHED") &&
                    !buffer.getStats().get("state").equals("FAILED") &&
                    dataIter.hasNext();
        }

        @Override
        public ColumnDescription next()
        {
            if (!dataIter.hasNext()) {
                buffer = waitForData(getNextUri(buffer.getNextUri()));
                dataIter = buffer.getData().iterator();
            }
            List<Object> row = dataIter.next();

            // Trino's DescribeOutput Schema
            //   Column Name  | Catalog | Schema | Table  |    Type     | Type Size | Aliased
            // ---------------+---------+--------+--------+-------------+-----------+---------
            //  orderkey      | tpch    | tiny   | orders | bigint      |         8 | false
            return new ColumnDescription((String) row.get(0), (String) row.get(4));
        }
    }
}
