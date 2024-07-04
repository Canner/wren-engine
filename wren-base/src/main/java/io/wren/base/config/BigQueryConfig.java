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
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

@Deprecated
public class BigQueryConfig
{
    public static final String BIGQUERY_CRENDITALS_KEY = "bigquery.credentials-key";
    public static final String BIGQUERY_CRENDITALS_FILE = "bigquery.credentials-file";
    public static final String BIGQUERY_PROJECT_ID = "bigquery.project-id";
    public static final String BIGQUERY_LOCATION = "bigquery.location";
    public static final String BIGQUERY_BUCKET_NAME = "bigquery.bucket-name";
    public static final String BIGQUERY_METADATA_SCHEMA_PREFIX = "bigquery.metadata.schema.prefix";
    private Optional<String> credentialsKey = Optional.empty();
    private Optional<String> credentialsFile = Optional.empty();
    private Optional<String> projectId = Optional.empty();

    private Optional<String> location = Optional.empty();

    private Optional<String> bucketName = Optional.empty();
    private String metadataSchemaPrefix = "";

    public Optional<String> getCredentialsKey()
    {
        return credentialsKey;
    }

    @Config(BIGQUERY_CRENDITALS_KEY)
    @ConfigDescription("The base64 encoded credentials key")
    @ConfigSecuritySensitive
    public BigQueryConfig setCredentialsKey(String credentialsKey)
    {
        this.credentialsKey = Optional.ofNullable(credentialsKey);
        return this;
    }

    public Optional<String> getCredentialsFile()
    {
        return credentialsFile;
    }

    @Config(BIGQUERY_CRENDITALS_FILE)
    @ConfigDescription("The path to the JSON credentials file")
    public BigQueryConfig setCredentialsFile(String credentialsFile)
    {
        this.credentialsFile = Optional.ofNullable(credentialsFile);
        return this;
    }

    public Optional<String> getProjectId()
    {
        return projectId;
    }

    @Config(BIGQUERY_PROJECT_ID)
    @ConfigDescription("The Google Cloud Project ID where the data reside")
    public BigQueryConfig setProjectId(String projectId)
    {
        this.projectId = Optional.ofNullable(projectId);
        return this;
    }

    public Optional<String> getLocation()
    {
        return location;
    }

    @Config(BIGQUERY_LOCATION)
    @ConfigDescription("The Google Cloud Project ID where the data reside")
    public BigQueryConfig setLocation(String location)
    {
        this.location = Optional.ofNullable(location);
        return this;
    }

    public Optional<String> getBucketName()
    {
        return bucketName;
    }

    @Config(BIGQUERY_BUCKET_NAME)
    @ConfigDescription("The Google Cloud bucket name used to temporarily store the metric cached results")
    public BigQueryConfig setBucketName(String bucketName)
    {
        this.bucketName = Optional.ofNullable(bucketName);
        return this;
    }

    @NotNull
    public String getMetadataSchemaPrefix()
    {
        return metadataSchemaPrefix;
    }

    @Config(BIGQUERY_METADATA_SCHEMA_PREFIX)
    @ConfigDescription("Wren needs to create two schemas in BigQuery: wren_temp, pg_catalog. This is a config to add a prefix to the names of these two schemas if it's set.")
    public BigQueryConfig setMetadataSchemaPrefix(String metadataSchemaPrefix)
    {
        this.metadataSchemaPrefix = metadataSchemaPrefix;
        return this;
    }
}
