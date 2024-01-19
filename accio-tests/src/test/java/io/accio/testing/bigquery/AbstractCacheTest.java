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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.accio.base.AccioMDL;
import io.accio.base.Column;
import io.accio.base.ConnectorRecordIterator;
import io.accio.base.client.AutoCloseableIterator;
import io.accio.base.client.duckdb.DuckdbClient;
import io.accio.base.dto.Manifest;
import io.accio.base.type.DateType;
import io.accio.base.type.PGType;
import io.accio.cache.CacheInfoPair;
import io.accio.cache.CacheManager;
import io.accio.cache.CachedTableMapping;
import io.accio.main.metadata.Metadata;
import io.accio.testing.TestingAccioServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.accio.base.Utils.randomIntString;
import static java.lang.String.format;
import static java.lang.System.getenv;

public abstract class AbstractCacheTest
        extends AbstractWireProtocolTestWithBigQuery
{
    protected final Supplier<CacheManager> cacheManager = () -> getInstance(Key.get(CacheManager.class));
    protected final Supplier<CachedTableMapping> cachedTableMapping = () -> getInstance(Key.get(CachedTableMapping.class));
    protected final Supplier<DuckdbClient> duckdbClient = () -> getInstance(Key.get(DuckdbClient.class));

    @Override
    protected TestingAccioServer createAccioServer()
    {
        try {
            Path dir = Files.createTempDirectory(getAccioDirectory());
            if (getAccioMDLPath().isPresent()) {
                Files.copy(Path.of(getAccioMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(AccioMDL.EMPTY.getManifest()));
            }
            return TestingAccioServer.builder()
                    .setRequiredConfigs(getProperties().put("accio.directory", dir.toString()).build())
                    .build();
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected ImmutableMap.Builder<String, String> getProperties()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("bigquery.bucket-name", getenv("TEST_BIG_QUERY_BUCKET_NAME"))
                .put("bigquery.metadata.schema.prefix", format("test_%s_", randomIntString()))
                .put("duckdb.storage.access-key", getenv("TEST_DUCKDB_STORAGE_ACCESS_KEY"))
                .put("duckdb.storage.secret-key", getenv("TEST_DUCKDB_STORAGE_SECRET_KEY"))
                .put("accio.datasource.type", "bigquery");

        return properties;
    }

    protected CacheInfoPair getDefaultCacheInfoPair(String name)
    {
        return cachedTableMapping.get().getCacheInfoPair("canner-cml", "tpch_tiny", name);
    }

    protected List<Object[]> queryDuckdb(String statement)
    {
        try (AutoCloseableIterator<Object[]> iterator = duckdbClient.get().query(statement)) {
            ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
            while (iterator.hasNext()) {
                builder.add(iterator.next());
            }
            return builder.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected List<Object[]> queryBigQuery(String statement)
    {
        Metadata bigQueryMetadata = getInstance(Key.get(Metadata.class));
        try (ConnectorRecordIterator iterator = bigQueryMetadata.directQuery(statement, List.of())) {
            List<PGType> pgTypes = iterator.getColumns().stream().map(Column::getType).collect(toImmutableList());
            ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
            while (iterator.hasNext()) {
                Object[] bqResults = iterator.next();
                Object[] returnResults = new Object[pgTypes.size()];
                for (int i = 0; i < pgTypes.size(); i++) {
                    returnResults[i] = bqResults[i];
                    if (DateType.DATE.oid() == pgTypes.get(i).oid()) {
                        returnResults[i] = LocalDate.parse(bqResults[i].toString());
                    }
                }
                builder.add(returnResults);
            }
            return builder.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
