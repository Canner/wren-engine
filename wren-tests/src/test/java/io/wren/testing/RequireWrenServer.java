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

package io.wren.testing;

import com.google.common.io.Closer;
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.wren.base.config.ConfigManager;
import io.wren.base.dto.Column;
import io.wren.base.dto.Manifest;
import io.wren.main.connector.duckdb.DuckDBMetadata;
import io.wren.main.validation.ValidationResult;
import io.wren.main.web.dto.DryPlanDto;
import io.wren.main.web.dto.DryPlanDtoV2;
import io.wren.main.web.dto.ErrorMessageDto;
import io.wren.main.web.dto.PreviewDto;
import io.wren.main.web.dto.QueryAnalysisDto;
import io.wren.main.web.dto.QueryResultDto;
import io.wren.main.web.dto.SqlAnalysisInputBatchDto;
import io.wren.main.web.dto.SqlAnalysisInputDto;
import io.wren.main.web.dto.ValidateDto;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.util.List;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePatch;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class RequireWrenServer
{
    protected TestingWrenServer wrenServer;
    protected Closer closer = Closer.create();
    protected HttpClient client;

    private static final JsonCodec<ErrorMessageDto> ERROR_CODEC = jsonCodec(ErrorMessageDto.class);
    public static final JsonCodec<Manifest> MANIFEST_JSON_CODEC = jsonCodec(Manifest.class);
    private static final JsonCodec<PreviewDto> PREVIEW_DTO_CODEC = jsonCodec(PreviewDto.class);
    private static final JsonCodec<SqlAnalysisInputDto> SQL_ANALYSIS_INPUT_DTO_CODEC = jsonCodec(SqlAnalysisInputDto.class);
    private static final JsonCodec<ConfigManager.ConfigEntry> CONFIG_ENTRY_JSON_CODEC = jsonCodec(ConfigManager.ConfigEntry.class);
    private static final JsonCodec<List<ConfigManager.ConfigEntry>> CONFIG_ENTRY_LIST_CODEC = listJsonCodec(ConfigManager.ConfigEntry.class);
    private static final JsonCodec<QueryResultDto> QUERY_RESULT_DTO_CODEC = jsonCodec(QueryResultDto.class);
    private static final JsonCodec<List<Column>> COLUMN_LIST_CODEC = listJsonCodec(Column.class);
    private static final JsonCodec<DryPlanDto> DRY_PLAN_DTO_CODEC = jsonCodec(DryPlanDto.class);
    private static final JsonCodec<DryPlanDtoV2> DRY_PLAN_DTO_V2_CODEC = jsonCodec(DryPlanDtoV2.class);
    private static final JsonCodec<List<ValidationResult>> VALIDATION_RESULT_LIST_CODEC = listJsonCodec(ValidationResult.class);
    private static final JsonCodec<ValidateDto> VALIDATE_DTO_CODEC = jsonCodec(ValidateDto.class);
    private static final JsonCodec<List<QueryAnalysisDto>> QUERY_ANALYSIS_DTO_LIST_CODEC = listJsonCodec(QueryAnalysisDto.class);
    private static final JsonCodec<SqlAnalysisInputBatchDto> SQL_ANALYSIS_INPUT_BATCH_DTO_CODEC = jsonCodec(SqlAnalysisInputBatchDto.class);
    private static final JsonCodec<List<List<QueryAnalysisDto>>> QUERY_ANALYSIS_DTO_LIST_LIST_CODEC = listJsonCodec(listJsonCodec(QueryAnalysisDto.class));

    public RequireWrenServer() {}

    @BeforeClass
    public void init()
            throws Exception
    {
        this.wrenServer = createWrenServer();
        this.client = closer.register(createHttpClient());
        closer.register(wrenServer);
        prepare();
    }

    protected static JettyHttpClient createHttpClient()
    {
        return new JettyHttpClient(new HttpClientConfig().setIdleTimeout(new Duration(20, SECONDS)));
    }

    protected abstract TestingWrenServer createWrenServer()
            throws Exception;

    protected void initDuckDB()
    {
        ClassLoader classLoader = getClass().getClassLoader();
        String initSQL;
        try {
            initSQL = Resources.toString(requireNonNull(classLoader.getResource("duckdb/init.sql")).toURI().toURL(), UTF_8);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        initSQL = initSQL.replaceAll("basePath", requireNonNull(classLoader.getResource("tpch/data")).getPath());
        DuckDBMetadata metadata = wrenServer.getInstance(Key.get(DuckDBMetadata.class));
        metadata.setInitSQL(initSQL);
        metadata.reload();
    }

    protected TestingWrenServer server()
    {
        return wrenServer;
    }

    protected void prepare() {}

    public <T> T getInstance(Key<T> key)
    {
        return wrenServer.getInstance(key);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws IOException
    {
        cleanup();
        closer.close();
    }

    protected void cleanup() {}

    public <T, E extends Exception> T executeHttpRequest(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        return client.execute(request, responseHandler);
    }

    protected QueryResultDto preview(PreviewDto previewDto)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/mdl/preview"))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(PREVIEW_DTO_CODEC, previewDto))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return QUERY_RESULT_DTO_CODEC.fromJson(response.getBody());
    }

    protected List<Column> dryRun(PreviewDto previewDto)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/mdl/dry-run"))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(PREVIEW_DTO_CODEC, previewDto))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return COLUMN_LIST_CODEC.fromJson(response.getBody());
    }

    protected String dryPlan(DryPlanDto dryPlanDto)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/mdl/dry-plan"))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(DRY_PLAN_DTO_CODEC, dryPlanDto))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return response.getBody();
    }

    protected String dryPlanV2(DryPlanDtoV2 dryPlanDto)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v2/mdl/dry-plan"))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(DRY_PLAN_DTO_V2_CODEC, dryPlanDto))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return response.getBody();
    }

    protected List<QueryAnalysisDto> getSqlAnalysis(SqlAnalysisInputDto inputDto)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/analysis/sql"))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(SQL_ANALYSIS_INPUT_DTO_CODEC, inputDto))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return QUERY_ANALYSIS_DTO_LIST_CODEC.fromJson(response.getBody());
    }

    protected List<List<QueryAnalysisDto>> getSqlAnalysisBatch(SqlAnalysisInputBatchDto inputBatchDto)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v2/analysis/sqls"))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(SQL_ANALYSIS_INPUT_BATCH_DTO_CODEC, inputBatchDto))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return QUERY_ANALYSIS_DTO_LIST_LIST_CODEC.fromJson(response.getBody());
    }

    protected List<ConfigManager.ConfigEntry> getConfigs()
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/config"))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return CONFIG_ENTRY_LIST_CODEC.fromJson(response.getBody());
    }

    protected ConfigManager.ConfigEntry getConfig(String configName)
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve(format("/v1/config/%s", configName)))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return CONFIG_ENTRY_JSON_CODEC.fromJson(response.getBody());
    }

    protected void resetConfig()
    {
        Request request = prepareDelete()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/config"))
                .setHeader(CONTENT_TYPE, "application/json")
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
    }

    protected void patchConfig(List<ConfigManager.ConfigEntry> configEntries)
    {
        Request request = preparePatch()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/config"))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(CONFIG_ENTRY_LIST_CODEC, configEntries))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
    }

    protected QueryResultDto queryDuckDB(String statement)
    {
        Request request = preparePost()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/data-source/duckdb/query"))
                .setBodyGenerator(createStaticBodyGenerator(statement, UTF_8))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return QUERY_RESULT_DTO_CODEC.fromJson(response.getBody());
    }

    protected String getDuckDBInitSQL()
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/data-source/duckdb/settings/init-sql"))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return response.getBody();
    }

    protected void setDuckDBInitSQL(String statement)
    {
        Request request = preparePut()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/data-source/duckdb/settings/init-sql"))
                .setBodyGenerator(createStaticBodyGenerator(statement, UTF_8))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
    }

    protected void appendToDuckDBInitSQL(String statement)
    {
        Request request = preparePatch()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/data-source/duckdb/settings/init-sql"))
                .setBodyGenerator(createStaticBodyGenerator(statement, UTF_8))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
    }

    protected String getDuckDBSessionSQL()
    {
        Request request = prepareGet()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/data-source/duckdb/settings/session-sql"))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return response.getBody();
    }

    protected void setDuckDBSessionSQL(String statement)
    {
        Request request = preparePut()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/data-source/duckdb/settings/session-sql"))
                .setBodyGenerator(createStaticBodyGenerator(statement, UTF_8))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
    }

    protected void appendToDuckDBSessionSQL(String statement)
    {
        Request request = preparePatch()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/data-source/duckdb/settings/session-sql"))
                .setBodyGenerator(createStaticBodyGenerator(statement, UTF_8))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
    }

    protected List<ValidationResult> validate(String ruleName, ValidateDto validateDto)
    {
        Request request = preparePost()
                .setUri(server().getHttpServerBasedUrl().resolve(format("/v1/mdl/validate/%s", ruleName)))
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(VALIDATE_DTO_CODEC, validateDto))
                .build();

        StringResponseHandler.StringResponse response = executeHttpRequest(request, createStringResponseHandler());
        if (response.getStatusCode() != 200) {
            getWebApplicationException(response);
        }
        return VALIDATION_RESULT_LIST_CODEC.fromJson(response.getBody());
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
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .entity(errorMessageDto)
                        .build());
    }
}
