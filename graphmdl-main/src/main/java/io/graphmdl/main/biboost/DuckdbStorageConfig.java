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

package io.graphmdl.main.biboost;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.util.Optional;

public class DuckdbStorageConfig
{
    private String endpoint = "storage.googleapis.com";
    private Optional<String> accessKey = Optional.empty();
    private Optional<String> secretKey = Optional.empty();
    private Optional<String> region = Optional.empty();
    private String urlStyle = "path";

    @Config("duckdb.storage.endpoint")
    @ConfigDescription("The storage endpoint; default is storage.googleapis.com")
    public DuckdbStorageConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    @Config("duckdb.storage.access-key")
    @ConfigDescription("The storage access key")
    @ConfigSecuritySensitive
    public DuckdbStorageConfig setAccessKey(String accessKey)
    {
        this.accessKey = Optional.of(accessKey);
        return this;
    }

    public Optional<String> getAccessKey()
    {
        return accessKey;
    }

    @Config("duckdb.storage.secret-key")
    @ConfigDescription("The storage secret key")
    @ConfigSecuritySensitive
    public DuckdbStorageConfig setSecretKey(String secretKey)
    {
        this.secretKey = Optional.of(secretKey);
        return this;
    }

    public Optional<String> getSecretKey()
    {
        return secretKey;
    }

    @Config("duckdb.storage.region")
    @ConfigDescription("The storage region")
    public DuckdbStorageConfig setRegion(String region)
    {
        this.region = Optional.of(region);
        return this;
    }

    public Optional<String> getRegion()
    {
        return region;
    }

    @Config("duckdb.storage.url-style")
    @ConfigDescription("The storage url style; default is path")
    public DuckdbStorageConfig setUrlStyle(String urlStyle)
    {
        this.urlStyle = urlStyle;
        return this;
    }

    public String getUrlStyle()
    {
        return urlStyle;
    }
}
