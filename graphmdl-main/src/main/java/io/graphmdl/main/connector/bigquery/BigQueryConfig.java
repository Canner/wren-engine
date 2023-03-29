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
package io.graphmdl.main.connector.bigquery;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;

import java.util.Optional;

public class BigQueryConfig
{
    private Optional<String> credentialsKey = Optional.empty();
    private Optional<String> credentialsFile = Optional.empty();
    private Optional<String> projectId = Optional.empty();
    private Optional<String> parentProjectId = Optional.empty();

    private Optional<String> location = Optional.empty();

    private Optional<String> bucketName = Optional.empty();

    public Optional<String> getCredentialsKey()
    {
        return credentialsKey;
    }

    @Config("bigquery.credentials-key")
    @ConfigDescription("The base64 encoded credentials key")
    @ConfigSecuritySensitive
    public BigQueryConfig setCredentialsKey(String credentialsKey)
    {
        this.credentialsKey = Optional.of(credentialsKey);
        return this;
    }

    public Optional<@FileExists String> getCredentialsFile()
    {
        return credentialsFile;
    }

    @Config("bigquery.credentials-file")
    @ConfigDescription("The path to the JSON credentials file")
    public BigQueryConfig setCredentialsFile(String credentialsFile)
    {
        this.credentialsFile = Optional.of(credentialsFile);
        return this;
    }

    public Optional<String> getProjectId()
    {
        return projectId;
    }

    @Config("bigquery.project-id")
    @ConfigDescription("The Google Cloud Project ID where the data reside")
    public BigQueryConfig setProjectId(String projectId)
    {
        this.projectId = Optional.of(projectId);
        return this;
    }

    public Optional<String> getParentProjectId()
    {
        return parentProjectId;
    }

    @Config("bigquery.parent-project-id")
    @ConfigDescription("The Google Cloud Project ID to bill for the export")
    public BigQueryConfig setParentProjectId(String parentProjectId)
    {
        this.parentProjectId = Optional.of(parentProjectId);
        return this;
    }

    public Optional<String> getLocation()
    {
        return location;
    }

    @Config("bigquery.location")
    @ConfigDescription("The Google Cloud Project ID where the data reside")
    public BigQueryConfig setLocation(String location)
    {
        this.location = Optional.of(location);
        return this;
    }

    public Optional<String> getBucketName()
    {
        return bucketName;
    }

    @Config("bigquery.bucket-name")
    @ConfigDescription("The Google Cloud storage bucket name to use for export")
    public BigQueryConfig setBucketName(String bucketName)
    {
        this.bucketName = Optional.of(bucketName);
        return this;
    }
}
