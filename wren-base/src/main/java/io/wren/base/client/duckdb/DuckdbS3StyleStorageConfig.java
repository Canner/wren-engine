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

package io.wren.base.client.duckdb;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.util.Optional;

import static java.lang.String.format;

public class DuckdbS3StyleStorageConfig
        implements CacheStorageConfig
{
    public static final String DUCKDB_STORAGE_ENDPOINT = "duckdb.storage.endpoint";
    public static final String DUCKDB_STORAGE_ACCESS_KEY = "duckdb.storage.access-key";
    public static final String DUCKDB_STORAGE_SECRET_KEY = "duckdb.storage.secret-key";
    public static final String DUCKDB_STORAGE_REGION = "duckdb.storage.region";
    public static final String DUCKDB_STORAGE_URL_STYLE = "duckdb.storage.url-style";

    // https://duckdb.org/docs/guides/import/s3_import.html
    private String endpoint = "storage.googleapis.com";
    private Optional<String> accessKey = Optional.empty();
    private Optional<String> secretKey = Optional.empty();
    private Optional<String> region = Optional.empty();
    private String urlStyle = "path";

    @Config(DUCKDB_STORAGE_ENDPOINT)
    @ConfigDescription("The storage endpoint; default is storage.googleapis.com")
    public DuckdbS3StyleStorageConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    @Config(DUCKDB_STORAGE_ACCESS_KEY)
    @ConfigDescription("The storage access key")
    @ConfigSecuritySensitive
    public DuckdbS3StyleStorageConfig setAccessKey(String accessKey)
    {
        this.accessKey = Optional.of(accessKey);
        return this;
    }

    public Optional<String> getAccessKey()
    {
        return accessKey;
    }

    @Config(DUCKDB_STORAGE_SECRET_KEY)
    @ConfigDescription("The storage secret key")
    @ConfigSecuritySensitive
    public DuckdbS3StyleStorageConfig setSecretKey(String secretKey)
    {
        this.secretKey = Optional.of(secretKey);
        return this;
    }

    public Optional<String> getSecretKey()
    {
        return secretKey;
    }

    @Config(DUCKDB_STORAGE_REGION)
    @ConfigDescription("The storage region")
    public DuckdbS3StyleStorageConfig setRegion(String region)
    {
        this.region = Optional.of(region);
        return this;
    }

    public Optional<String> getRegion()
    {
        return region;
    }

    @Config(DUCKDB_STORAGE_URL_STYLE)
    @ConfigDescription("The storage url style; default is path")
    public DuckdbS3StyleStorageConfig setUrlStyle(String urlStyle)
    {
        this.urlStyle = urlStyle;
        return this;
    }

    public String getUrlStyle()
    {
        return urlStyle;
    }

    @Override
    public String generateDuckdbParquetStatement(String path, String tableName)
    {
        // ref: https://github.com/duckdb/duckdb/issues/1403
        StringBuilder sb = new StringBuilder();
        // TODO: check why can't we set s3 access key and secret key in Data source
        accessKey.ifPresent(accessKey -> sb.append(format("SET s3_access_key_id='%s';\n", accessKey)));
        secretKey.ifPresent(secretKey -> sb.append(format("SET s3_secret_access_key='%s';\n", secretKey)));
        sb.append("BEGIN TRANSACTION;\n");
        sb.append(format("CREATE TABLE \"%s\" AS SELECT * FROM read_parquet('s3://%s');", tableName, path));
        sb.append("COMMIT;\n");
        return sb.toString();
    }
}
