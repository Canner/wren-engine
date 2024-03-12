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
package io.accio.testing.bigquery;

import com.google.cloud.bigquery.DatasetId;
import com.google.inject.Key;
import io.accio.connector.bigquery.BigQueryClient;
import io.accio.main.metadata.Metadata;
import io.accio.testing.AbstractCacheTest;
import io.airlift.log.Logger;

public abstract class AbstractCacheTestWithBigQuery
        extends AbstractCacheTest
{
    private static final Logger LOG = Logger.get(AbstractCacheTestWithBigQuery.class);

    @Override
    protected String getDefaultCatalog()
    {
        return "canner-cml";
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
            Metadata metadata = getInstance(Key.get(Metadata.class));
            BigQueryClient bigQueryClient = getInstance(Key.get(BigQueryClient.class));
            bigQueryClient.dropDatasetWithAllContent(DatasetId.of(getDefaultCatalog(), metadata.getPgCatalogName()));
            bigQueryClient.dropDatasetWithAllContent(DatasetId.of(getDefaultCatalog(), metadata.getMetadataSchemaName()));
        }
        catch (Exception ex) {
            LOG.error(ex, "cleanup bigquery schema failed");
        }
    }
}
