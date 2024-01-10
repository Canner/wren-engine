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
import io.accio.base.CatalogSchemaTableName;
import io.accio.base.dto.Manifest;
import io.accio.cache.TaskInfo;
import io.accio.main.web.dto.CheckOutputDto;
import io.accio.main.web.dto.ErrorMessageDto;
import io.accio.main.web.dto.PreviewDto;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.accio.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

public abstract class RequireAccioServer
{
    private TestingAccioServer accioServer;
    protected Closer closer = Closer.create();
    protected HttpClient client;

    public static final JsonCodec<TaskInfo> TASK_INFO_CODEC = jsonCodec(TaskInfo.class);
    public static final JsonCodec<List<TaskInfo>> TASK_INFO_LIST_CODEC = listJsonCodec(TaskInfo.class);
    private static final JsonCodec<ErrorMessageDto> ERROR_CODEC = jsonCodec(ErrorMessageDto.class);
    private static final JsonCodec<CheckOutputDto> CHECK_OUTPUT_DTO_CODEC = jsonCodec(CheckOutputDto.class);
    private static final JsonCodec<PreviewDto> PREVIEW_DTO_CODEC = jsonCodec(PreviewDto.class);

    public RequireAccioServer() {}

    @BeforeClass
    public void init()
    {
        this.accioServer = createAccioServer();
        this.client = closer.register(new JettyHttpClient(new HttpClientConfig().setIdleTimeout(new Duration(300, SECONDS))));
        closer.register(accioServer);
        prepare();
    }

    protected abstract TestingAccioServer createAccioServer();

    protected TestingAccioServer server()
    {
        return accioServer;
    }

    protected void prepare() {}

    public <T> T getInstance(Key<T> key)
    {
        return accioServer.getInstance(key);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws IOException
    {
        cleanup();
        closer.close();
    }

    protected void cleanup() {}

    protected TaskInfo getTaskInfo(CatalogSchemaTableName name)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve(format("/v1/cache/info/%s/%s/%s",
                        name.getCatalogName(), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName())))
                .build();
        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return TASK_INFO_CODEC.fromJson(response.getBody());
    }

    protected List<TaskInfo> getTaskInfo(String catalog, String schema)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve(format("/v1/cache/info/%s/%s",
                        catalog, schema)))
                .build();
        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return TASK_INFO_LIST_CODEC.fromJson(response.getBody());
    }

    public <T, E extends Exception> T executeHttpRequest(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        return client.execute(request, responseHandler);
    }

    protected List<Object[]> preview(PreviewDto previewDto)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/mdl/preview"))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(PREVIEW_DTO_CODEC, previewDto))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return JsonCodec.listJsonCodec(Object[].class).fromJson(response.getBody());
    }

    protected void deployMDL(Manifest manifest)
    {
        Request request = preparePost()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/mdl/deploy"))
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(MANIFEST_JSON_CODEC, manifest))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 202) {
            getWebApplicationException(response);
        }
    }

    protected CheckOutputDto getDeployStatus()
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/mdl/status"))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return CHECK_OUTPUT_DTO_CODEC.fromJson(response.getBody());
    }

    protected void waitUntilReady()
            throws ExecutionException, InterruptedException, TimeoutException
    {
        CompletableFuture.runAsync(() -> {
            while (true) {
                CheckOutputDto checkOutputDto = getDeployStatus();
                if (checkOutputDto.getStatus() == CheckOutputDto.Status.READY) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    throw new AssertionError("Status doesn't change to READY", e);
                }
            }
        }).get(60, TimeUnit.SECONDS);
    }

    public static void getWebApplicationException(StringResponseHandler.StringResponse response)
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
