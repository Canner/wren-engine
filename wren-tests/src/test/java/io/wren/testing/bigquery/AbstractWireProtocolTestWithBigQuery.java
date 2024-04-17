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

package io.wren.testing.bigquery;

import com.google.cloud.bigquery.DatasetId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.airlift.log.Logger;
import io.wren.base.dto.Manifest;
import io.wren.connector.bigquery.BigQueryClient;
import io.wren.main.connector.bigquery.BigQueryMetadata;
import io.wren.testing.AbstractWireProtocolTest;
import io.wren.testing.TestingWrenServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.wren.base.Utils.randomIntString;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;

public abstract class AbstractWireProtocolTestWithBigQuery
        extends AbstractWireProtocolTest
{
    protected static final Manifest DEFAULT_MANIFEST = Manifest.builder()
            .setCatalog("wrenai")
            .setSchema("tpch_tiny")
            .build();
    private static final Logger LOG = Logger.get(AbstractWireProtocolTestWithBigQuery.class);

    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("bigquery.metadata.schema.prefix", format("test_%s_", randomIntString()))
                .put("pg-wire-protocol.auth.file", requireNonNull(getClass().getClassLoader().getResource("accounts")).getPath())
                .put("wren.datasource.type", "bigquery");

        try {
            Path dir = Files.createTempDirectory(getWrenDirectory());
            if (getWrenMDLPath().isPresent()) {
                Files.copy(Path.of(getWrenMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(DEFAULT_MANIFEST));
            }
            properties.put("wren.directory", dir.toString());
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    protected Optional<String> getWrenMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("tpch_mdl.json")).getPath());
    }

    @Override
    protected String getDefaultCatalog()
    {
        return "wrenai";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tpch_tiny";
    }

    @Override
    protected void cleanup()
    {
        try {
            BigQueryMetadata metadata = getInstance(Key.get(BigQueryMetadata.class));
            BigQueryClient bigQueryClient = metadata.getBigQueryClient();
            bigQueryClient.dropDatasetWithAllContent(DatasetId.of(getDefaultCatalog(), metadata.getPgCatalogName()));
            bigQueryClient.dropDatasetWithAllContent(DatasetId.of(getDefaultCatalog(), metadata.getMetadataSchemaName()));
        }
        catch (Exception ex) {
            LOG.error(ex, "cleanup bigquery schema failed");
        }
    }
}
