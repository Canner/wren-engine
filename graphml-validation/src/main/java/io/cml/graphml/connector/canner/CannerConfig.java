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

package io.cml.graphml.connector.canner;

import io.airlift.configuration.Config;

public class CannerConfig
{
    private String personalAccessToken;
    private String cannerUrl;
    private String pgPort = "7432";
    private String trinoPort = "8080";
    private String availableWorkspace;

    public String getPersonalAccessToken()
    {
        return personalAccessToken;
    }

    @Config("canner.pat")
    public CannerConfig setPersonalAccessToken(String personalAccessToken)
    {
        this.personalAccessToken = personalAccessToken;
        return this;
    }

    public String getCannerUrl()
    {
        return cannerUrl;
    }

    @Config("canner.url")
    public CannerConfig setCannerUrl(String cannerUrl)
    {
        this.cannerUrl = cannerUrl;
        return this;
    }

    public String getAvailableWorkspace()
    {
        return availableWorkspace;
    }

    @Config("canner.availableWorkspace")
    public CannerConfig setAvailableWorkspace(String availableWorkspace)
    {
        this.availableWorkspace = availableWorkspace;
        return this;
    }

    public String getPgPort()
    {
        return pgPort;
    }

    @Config("canner.pg-port")
    public CannerConfig setPgPort(String pgPort)
    {
        this.pgPort = pgPort;
        return this;
    }

    public String getTrinoPort()
    {
        return trinoPort;
    }

    @Config("canner.trino-port")
    public CannerConfig setTrinoPort(String trinoPort)
    {
        this.trinoPort = trinoPort;
        return this;
    }
}
