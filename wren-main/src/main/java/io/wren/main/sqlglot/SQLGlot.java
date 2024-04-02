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

package io.wren.main.sqlglot;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.wren.base.config.SQLGlotConfig;
import io.wren.main.sqlglot.dto.TranspileDTO;

import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.UriBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class SQLGlot
        implements Closeable
{
    public enum Dialect
    {
        BIGQUERY("bigquery"),
        DUCKDB("duckdb"),
        POSTGRES("postgres"),
        SNOWFLAKE("snowflake"),
        TRINO("trino");

        private final String dialect;

        Dialect(String dialect)
        {
            this.dialect = dialect;
        }

        public String getDialect()
        {
            return dialect;
        }
    }

    private static final JsonCodec<TranspileDTO> TRANSPILE_DTO_JSON_CODEC = jsonCodec(TranspileDTO.class);

    private final URI baseUri;
    private final HttpClient client;

    @Inject
    public SQLGlot(SQLGlotConfig config)
    {
        this.baseUri = getBaseUri(config);
        this.client = new JettyHttpClient(new HttpClientConfig().setIdleTimeout(new Duration(20, SECONDS)));
    }

    @Override
    public void close()
    {
        client.close();
    }

    public String transpile(String sql, Dialect read, Dialect write)
            throws IOException
    {
        Request request = preparePost()
                .setUri(UriBuilder.fromUri(baseUri).path("sqlglot").path("transpile").build())
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(TRANSPILE_DTO_JSON_CODEC, new TranspileDTO(sql, read.getDialect(), write.getDialect())))
                .build();

        StringResponseHandler.StringResponse response = client.execute(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throwWebApplicationException(response);
        }
        return TRANSPILE_DTO_JSON_CODEC.fromJson(response.getBody()).getSql();
    }

    private static void throwWebApplicationException(StringResponseHandler.StringResponse response)
    {
        throw new WebApplicationException(response.getBody());
    }

    public static URI getBaseUri(SQLGlotConfig config)
    {
        return URI.create("http://0.0.0.0:" + config.getPort());
    }
}
