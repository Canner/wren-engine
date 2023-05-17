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

import io.airlift.log.Logger;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.connector.bigquery.GcsStorageClient;
import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.preaggregation.PreAggregationService;

import javax.inject.Inject;

import java.util.Optional;

import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class BigQueryPreAggregationService
        implements PreAggregationService
{
    private static final Logger LOG = Logger.get(BigQueryPreAggregationService.class);
    private static final String PRE_AGGREGATION_FOLDER = format("pre-agg-%s", randomUUID());
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
    public String createPreAggregation(String catalog, String schema, String name, String statement)
    {
        String path = format("%s/%s/%s/%s/%s/%s/*.parquet",
                getRequiredBucketName(),
                PRE_AGGREGATION_FOLDER,
                catalog,
                schema,
                name,
                randomUUID());
        String exportStatement = format("EXPORT DATA OPTIONS(\n" +
                        "  uri='gs://%s',\n" +
                        "  format='PARQUET',\n" +
                        "  overwrite=true) AS %s",
                path,
                statement);
        metadata.directDDL(exportStatement);
        return path;
    }

    @Override
    public void cleanPreAggregation()
    {
        bucketName.ifPresent(bucket -> {
            if (!gcsStorageClient.cleanFolders(bucket, PRE_AGGREGATION_FOLDER)) {
                LOG.error("Failed to clean pre-aggregation folder. Please check the bucket %s", getRequiredBucketName());
            }
        });
    }

    public String getRequiredBucketName()
    {
        return bucketName.orElseThrow(() -> new GraphMDLException(GENERIC_USER_ERROR, "Bucket name must be set"));
    }
}
