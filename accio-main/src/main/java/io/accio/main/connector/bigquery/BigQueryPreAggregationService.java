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
package io.accio.main.connector.bigquery;

import io.accio.base.AccioException;
import io.accio.connector.bigquery.GcsStorageClient;
import io.accio.main.metadata.Metadata;
import io.accio.preaggregation.PathInfo;
import io.accio.preaggregation.PreAggregationService;

import javax.inject.Inject;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.accio.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class BigQueryPreAggregationService
        implements PreAggregationService
{
    private static final String PRE_AGGREGATION_FOLDER = format("pre-agg-%s", randomUUID());
    // Pattern: bucket/PRE_AGGREGATION_FOLDER/catalog/schema/name/uuid
    private static final Pattern PATH_PATTERN = Pattern.compile(
            ".+/(?<preAggFolder>pre-agg-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/(?<catalog>[-_a-z0-9]+)/(?<schema>[-_a-z0-9]+)/(?<metricName>[-_a-zA-Z0-9]+)/(?<randomTail>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})");
    private static final String PRE_AGGREGATION_FOLDER_GROUP = "preAggFolder";
    private static final String CATALOG_GROUP = "catalog";
    private static final String SCHEMA_GROUP = "schema";
    private static final String METRIC_NAME_GROUP = "metricName";
    private static final String RANDOM_TAIL_GROUP = "randomTail";

    private final Optional<String> bucketName;
    private final Metadata metadata;
    private final GcsStorageClient gcsStorageClient;

    @Inject
    public BigQueryPreAggregationService(
            Metadata metadata,
            BigQueryConfig bigQueryConfig,
            GcsStorageClient gcsStorageClient)
    {
        requireNonNull(bigQueryConfig, "bigQueryConfig is null");
        this.bucketName = bigQueryConfig.getBucketName();
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.gcsStorageClient = requireNonNull(gcsStorageClient, "gcsStorageClient is null");
    }

    @Override
    public Optional<PathInfo> createPreAggregation(String catalog, String schema, String name, String statement)
    {
        String path = format("%s/%s/%s/%s/%s/%s",
                getRequiredBucketName(),
                PRE_AGGREGATION_FOLDER,
                catalog,
                schema,
                name,
                randomUUID());
        String pattern = "*.parquet";
        String exportStatement = format("EXPORT DATA OPTIONS(\n" +
                        "  uri='gs://%s/%s',\n" +
                        "  format='PARQUET',\n" +
                        "  overwrite=true) AS %s",
                path,
                pattern,
                statement);
        metadata.directDDL(exportStatement);
        return Optional.of(PathInfo.of(path, pattern));
    }

    @Override
    public void deleteTarget(PathInfo pathInfo)
    {
        getTableLocationPrefix(pathInfo.getPath())
                .ifPresent(prefix -> gcsStorageClient.cleanFolders(getRequiredBucketName(), prefix));
    }

    public static Optional<String> getTableLocationPrefix(String path)
    {
        Matcher matcher = PATH_PATTERN.matcher(path);
        if (matcher.matches()) {
            return Optional.of(format("%s/%s/%s/%s/%s",
                    matcher.group(PRE_AGGREGATION_FOLDER_GROUP),
                    matcher.group(CATALOG_GROUP),
                    matcher.group(SCHEMA_GROUP),
                    matcher.group(METRIC_NAME_GROUP),
                    matcher.group(RANDOM_TAIL_GROUP)));
        }
        return Optional.empty();
    }

    public String getRequiredBucketName()
    {
        return bucketName.orElseThrow(() -> new AccioException(GENERIC_USER_ERROR, "Bucket name must be set"));
    }
}
