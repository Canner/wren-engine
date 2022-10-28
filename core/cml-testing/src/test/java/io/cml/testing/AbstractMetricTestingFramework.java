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

package io.cml.testing;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.cml.metrics.Metric;
import io.cml.testing.bigquery.BigQueryTesting;
import io.cml.web.dto.ErrorMessageDto;
import io.cml.web.dto.MetricDto;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.util.List;

import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public class AbstractMetricTestingFramework
        extends RequireCmlServer
        implements BigQueryTesting
{
    private static final long QUERY_TIMEOUT_SECONDS = parseLong(getenv().getOrDefault("TEST_QUERY_TIMEOUT", "20"));
    private static final JsonCodec<Metric> METRIC_JSON_CODEC = jsonCodec(Metric.class);
    private static final JsonCodec<MetricDto> METRIC_DTO_JSON_CODEC = jsonCodec(MetricDto.class);

    private static final JsonCodec<List<MetricDto>> LIST_METRIC_DTO_JSON_CODEC = listJsonCodec(MetricDto.class);
    private static final JsonCodec<ErrorMessageDto> ERROR_MESSAGE_DTO_JSON_CODEC = jsonCodec(ErrorMessageDto.class);
    protected static final Logger LOG = Logger.get(AbstractMetricTestingFramework.class);
    protected HttpClient httpClient;

    public AbstractMetricTestingFramework()
    {
        super();
        this.httpClient = closer.register(new JettyHttpClient(new HttpClientConfig().setIdleTimeout(new Duration(QUERY_TIMEOUT_SECONDS, SECONDS))));
    }

    @Override
    protected TestingCmlServer createCmlServer()
    {
        return createCmlServerWithBigQuery();
    }

    public void createMetric(Metric metric)
    {
        Request request = preparePost()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/metric"))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(METRIC_JSON_CODEC, metric))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }
    }

    public void updateMetric(Metric metric)
    {
        Request request = preparePut()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/metric/" + metric.getName()))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(METRIC_JSON_CODEC, metric))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }
    }

    public void dropMetric(String metricName)
    {
        Request request = prepareDelete()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/metric/" + metricName))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }
    }

    public MetricDto getOneMetric(String metricName)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/metric/" + metricName))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }
        return METRIC_DTO_JSON_CODEC.fromJson(response.getBody());
    }

    public List<MetricDto> getAllMetrics()
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/metric"))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            throw getWebApplicationException(response);
        }
        return LIST_METRIC_DTO_JSON_CODEC.fromJson(response.getBody());
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

        LOG.error(response.getStatusCode() + " " + errorMessageDto);
        throw new WebApplicationException(
                Response.status(response.getStatusCode())
                        .type(APPLICATION_JSON)
                        .entity(errorMessageDto)
                        .build());
    }
}
