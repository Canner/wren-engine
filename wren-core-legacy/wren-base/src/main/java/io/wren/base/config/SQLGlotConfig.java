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

package io.wren.base.config;

import io.airlift.configuration.Config;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;

import static java.lang.System.getenv;

@Deprecated
public class SQLGlotConfig
{
    public static final String SQLGLOT_PORT = "sqlglot.port";

    private int port = Optional.ofNullable(getenv("SQLGLOT_PORT"))
            .map(Integer::parseInt)
            .orElse(8000);

    public int getPort()
    {
        return port;
    }

    @Config(SQLGLOT_PORT)
    public void setPort(int port)
    {
        this.port = port;
    }

    public static SQLGlotConfig createConfigWithFreePort()
    {
        SQLGlotConfig config = new SQLGlotConfig();
        config.setPort(findFreePort());
        return config;
    }

    private static int findFreePort()
    {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
        catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
