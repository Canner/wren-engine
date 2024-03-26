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

package io.wren.sqlglot.glot;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.wren.sqlglot.dto.TranspileDTO;

import javax.ws.rs.WebApplicationException;

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
        TRINO("trino"),
        BIGQUERY("bigquery"),
        POSTGRES("postgres"),
        DUCKDB("duckdb");

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
    private static final URI BASE_URL = URI.create("http://0.0.0.0:8000/sqlglot/");

    private final HttpClient client;

    public SQLGlot()
    {
        this.client = new JettyHttpClient(new HttpClientConfig().setIdleTimeout(new Duration(20, SECONDS)));
    }

    public String transpile(String sql, Dialect read, Dialect write)
            throws IOException
    {
        return post("transpile", sql, read, write);
    }

    @Override
    public void close()
    {
        client.close();
    }

    private String post(String api, String sql, Dialect read, Dialect write)
    {
        Request request = preparePost()
                .setUri(BASE_URL.resolve(api))
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(TRANSPILE_DTO_JSON_CODEC, new TranspileDTO(sql, read.getDialect(), write.getDialect())))
                .build();
        StringResponseHandler.StringResponse response = client.execute(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throwWebApplicationException(response);
        }
        return response.getBody().replaceAll("\"", "");
    }

    private static void throwWebApplicationException(StringResponseHandler.StringResponse response)
    {
        throw new WebApplicationException(response.getBody());
    }
}
