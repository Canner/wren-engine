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

package io.wren.main.server;

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.lang.String.format;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

public class Server
{
    public final void start()
    {
        doStart();
    }

    private void doStart()
    {
        Logger log = Logger.get(Server.class);
        log.info("Java version: %s", StandardSystemProperty.JAVA_VERSION.value());

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add();
        modules.addAll(getAdditionalModules());
        Bootstrap app = new Bootstrap(modules.build());

        try {
            Injector injector = app.initialize();

            configure(injector);

            log.info("======== SERVER STARTED ========");
        }
        catch (ApplicationConfigurationException e) {
            StringBuilder message = new StringBuilder();
            message.append("Configuration is invalid\n");
            message.append("==========\n");
            addMessages(message, "Errors", ImmutableList.copyOf(e.getErrors()));
            addMessages(message, "Warnings", ImmutableList.copyOf(e.getWarnings()));
            message.append("\n");
            message.append("==========");
            log.error(message.toString());
            System.exit(1);
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }

    protected Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of();
    }

    protected void configure(Injector injector) {}

    private static void addMessages(StringBuilder output, String type, List<Object> messages)
    {
        if (messages.isEmpty()) {
            return;
        }
        output.append("\n").append(type).append(":\n\n");
        for (int index = 0; index < messages.size(); index++) {
            output.append(format("%s) %s\n", index + 1, messages.get(index)));
        }
    }

    private static void logLocation(Logger log, String name, Path path)
    {
        if (!Files.exists(path, NOFOLLOW_LINKS)) {
            log.info("%s: [does not exist]", name);
            return;
        }
        try {
            path = path.toAbsolutePath().toRealPath();
        }
        catch (IOException e) {
            log.info("%s: [not accessible]", name);
            return;
        }
        log.info("%s: %s", name, path);
    }
}
