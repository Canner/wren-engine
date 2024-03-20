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

package io.wren.main.wireprotocol.auth;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

public class FileAuthentication
        implements Authentication
{
    private static final Logger LOG = Logger.get(FileAuthentication.class);
    private final Map<String, String> accounts;

    public FileAuthentication(File authFile)
    {
        this.accounts = authFile.isFile() ? parseAuthFile(authFile) : ImmutableMap.of();
        if (accounts.isEmpty()) {
            LOG.warn("No accounts found in auth file, disable the authentication");
        }
    }

    @Override
    public boolean isEnabled()
    {
        return !accounts.isEmpty();
    }

    @Override
    public boolean authenticate(String username, String password)
    {
        if (accounts.isEmpty()) {
            return true;
        }

        return accounts.containsKey(username) &&
                accounts.get(username).equals(password);
    }

    private static Map<String, String> parseAuthFile(File authFile)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        try {
            Files.readAllLines(authFile.toPath()).forEach(line -> {
                String[] parts = line.split(" ");
                if (parts.length != 2) {
                    LOG.warn("Invalid line %s in auth file %s", line, authFile);
                    return;
                }
                builder.put(parts[0], parts[1]);
            });
        }
        catch (Exception e) {
            LOG.error(e, "Failed to parse auth file %s", authFile);
        }
        return builder.build();
    }
}
