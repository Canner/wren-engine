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

package io.accio.testing;

import com.google.common.io.Closer;
import com.google.inject.Key;
import io.accio.main.web.dto.ErrorMessageDto;
import io.accio.preaggregation.TaskInfo;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.io.IOException;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public abstract class RequireAccioServer
{
    private final TestingAccioServer accioServer;
    protected final Closer closer = Closer.create();
    protected final HttpClient client;

    public static final JsonCodec<TaskInfo> TASK_INFO_CODEC = JsonCodec.jsonCodec(TaskInfo.class);
    private static final JsonCodec<ErrorMessageDto> ERROR_CODEC = jsonCodec(ErrorMessageDto.class);

    public RequireAccioServer()
    {
        this.accioServer = createAccioServer();
        this.client = closer.register(new JettyHttpClient(new HttpClientConfig().setIdleTimeout(new Duration(20, SECONDS))));
        closer.register(accioServer);
    }

    protected abstract TestingAccioServer createAccioServer();

    protected TestingAccioServer server()
    {
        return accioServer;
    }

    public <T> T getInstance(Key<T> key)
    {
        return accioServer.getInstance(key);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws IOException
    {
        closer.close();
    }

    protected void reloadAccioMDL()
    {
        Request request = preparePut()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/reload"))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());

        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
    }

    protected void reloadPreAggregation()
    {
        Request request = preparePut()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/preAggregation/reload"))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());

        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
    }

    protected TaskInfo reloadPreAggregationAsync()
    {
        Request request = preparePost()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/preAggregation/reload/async"))
                .build();
        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return TASK_INFO_CODEC.fromJson(response.getBody());
    }

    protected TaskInfo getTaskInfoByTaskId(String taskId)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/preAggregation/reload/async/" + taskId))
                .build();
        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return TASK_INFO_CODEC.fromJson(response.getBody());
    }

    public <T, E extends Exception> T executeHttpRequest(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        return client.execute(request, responseHandler);
    }

    public static WebApplicationException getWebApplicationException(StringResponseHandler.StringResponse response)
    {
        String body = response.getBody();
        ErrorMessageDto errorMessageDto;
        try {
            errorMessageDto = ERROR_CODEC.fromJson(body);
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
}
