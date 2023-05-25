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

package io.graphmdl.testing;

import com.google.common.io.Closer;
import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;

import java.io.IOException;

import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class RequireGraphMDLServer
{
    private final TestingGraphMDLServer graphMDLServer;
    protected final Closer closer = Closer.create();
    protected final HttpClient client;

    public RequireGraphMDLServer()
    {
        this.graphMDLServer = createGraphMDLServer();
        this.client = closer.register(new JettyHttpClient(new HttpClientConfig().setIdleTimeout(new Duration(20, SECONDS))));
        closer.register(graphMDLServer);
    }

    protected abstract TestingGraphMDLServer createGraphMDLServer();

    protected TestingGraphMDLServer server()
    {
        return graphMDLServer;
    }

    public <T> T getInstance(Key<T> key)
    {
        return graphMDLServer.getInstance(key);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws IOException
    {
        closer.close();
    }

    protected void reloadGraphMDL()
    {
        Request request = preparePut()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/reload"))
                .build();

        try (HttpClient client = new JettyHttpClient()) {
            StringResponseHandler.StringResponse response = client.execute(request, createStringResponseHandler());
            if (response.getStatusCode() != 200) {
                throw new RuntimeException(response.getBody());
            }
        }
    }

    protected void reloadPreAggregation()
    {
        Request request = preparePut()
                .setUri(server().getHttpServerBasedUrl().resolve("/v1/preAggregation/reload"))
                .build();

        try (HttpClient client = new JettyHttpClient()) {
            StringResponseHandler.StringResponse response = client.execute(request, createStringResponseHandler());
            if (response.getStatusCode() != 200) {
                throw new RuntimeException(response.getBody());
            }
        }
    }
}
