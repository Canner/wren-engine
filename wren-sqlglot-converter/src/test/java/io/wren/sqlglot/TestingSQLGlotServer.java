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

package io.wren.sqlglot;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;

public class TestingSQLGlotServer
        implements Closeable
{
    private final Process process;

    public TestingSQLGlotServer()
    {
        ProcessBuilder processBuilder = new ProcessBuilder(
                "python",
                "/Users/grieve/CannerData/wren-engine/wren-sqlglot-server/main.py");

        processBuilder.redirectErrorStream(true);

        try {
            process = processBuilder.start();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        waitASecond();
        // waitReady();
    }

    @Override
    public void close()
    {
        process.destroy();
    }

    private void waitASecond()
    {
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void waitReady()
    {
        HttpClient client = new JettyHttpClient();
        Request request = prepareGet()
                .setUri(URI.create("http://0.0.0.0:8000/ready"))
                .build();
        while (true) {
            try {
                StringResponseHandler.StringResponse response = client.execute(request, createStringResponseHandler());
                if (response.getStatusCode() == 200) {
                    break;
                }
            }
            catch (Exception e) {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException interruptedException) {
                    throw new RuntimeException(interruptedException);
                }
            }
        }
    }
}
