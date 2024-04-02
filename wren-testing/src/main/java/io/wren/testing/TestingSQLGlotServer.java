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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.wren.base.config.SQLGlotConfig;
import io.wren.main.sqlglot.SQLGlot;

import javax.annotation.PreDestroy;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestingSQLGlotServer
        implements Closeable
{
    private final SQLGlotConfig config;
    private final Path workingDirectory;
    private final File sourceCodeDirectory = new File("../wren-sqlglot-server").getAbsoluteFile();
    private final Process process;

    public TestingSQLGlotServer(SQLGlotConfig config)
    {
        this.config = config;

        try {
            this.workingDirectory = Files.createTempDirectory("testing-sqlglot-venv");

            createVirtualEnvironment(getPythonCommand());

            installDependencies();

            ProcessBuilder processBuilder = new ProcessBuilder(resolve("bin", "python"), "main.py");
            processBuilder.directory(sourceCodeDirectory);
            processBuilder.inheritIO();

            Map<String, String> env = processBuilder.environment();
            env.put("SQLGLOT_PORT", Integer.toString(config.getPort()));
            env.put("SQLGLOT_LOG_LEVEL", "DEBUG");

            process = processBuilder.start();
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        waitReady();
    }

    @PreDestroy
    @Override
    public void close()
    {
        process.destroyForcibly();
        workingDirectory.toFile().delete();
    }

    private String getPythonCommand()
    {
        return Stream.of("python", "python3")
                .filter(command -> {
                    try {
                        Process process = new ProcessBuilder(command, "--version").start();
                        process.waitFor();
                        return true;
                    }
                    catch (IOException | InterruptedException e) {
                        // Ignore the exception and try the next one
                        return false;
                    }
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Python not found"));
    }

    private void createVirtualEnvironment(String python)
            throws IOException, InterruptedException
    {
        boolean success = new ProcessBuilder(python, "-m", "venv", workingDirectory.toString()).start().waitFor(10, SECONDS);
        if (!success) {
            throw new RuntimeException("Failed to create virtual environment");
        }
    }

    private void installDependencies()
            throws IOException, InterruptedException
    {
        ProcessBuilder processBuilder = new ProcessBuilder(resolve("bin", "python"), "-m", "pip", "install", "-r", "requirements.txt");
        processBuilder.directory(sourceCodeDirectory);
        boolean success = processBuilder.start().waitFor(10, SECONDS);
        if (!success) {
            throw new RuntimeException("Failed to install dependencies");
        }
    }

    private void waitReady()
    {
        try (HttpClient client = new JettyHttpClient()) {
            Request request = prepareGet()
                    .setUri(SQLGlot.getBaseUri(config))
                    .build();

            for (int i = 0; i < 10; i++) {
                try {
                    Thread.sleep(1000);
                    StringResponseHandler.StringResponse response = client.execute(request, StringResponseHandler.createStringResponseHandler());
                    if (response.getStatusCode() == 200) {
                        return;
                    }
                }
                catch (Exception e) {
                    // Ignore the exception and try again
                }
            }
            throw new RuntimeException("SQLGlot server is not ready");
        }
    }

    private String resolve(String... folderPath)
    {
        Path path = workingDirectory;
        for (String p : folderPath) {
            path = path.resolve(p);
        }
        return path.toString();
    }
}
