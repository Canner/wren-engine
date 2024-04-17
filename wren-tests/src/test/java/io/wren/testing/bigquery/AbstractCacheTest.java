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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.wren.base.Column;
import io.wren.base.ConnectorRecordIterator;
import io.wren.base.WrenMDL;
import io.wren.base.dto.CacheInfo;
import io.wren.base.dto.Manifest;
import io.wren.base.type.DateType;
import io.wren.base.type.PGType;
import io.wren.base.wireprotocol.PgMetastore;
import io.wren.cache.CacheInfoPair;
import io.wren.cache.CacheManager;
import io.wren.cache.CachedTableMapping;
import io.wren.main.WrenMetastore;
import io.wren.main.metadata.Metadata;
import io.wren.testing.TestingWrenServer;
import org.testng.annotations.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.wren.base.Utils.randomIntString;
import static java.lang.String.format;
import static java.lang.System.getenv;

public abstract class AbstractCacheTest
        extends AbstractWireProtocolTestWithBigQuery
{
    protected final Supplier<CacheManager> cacheManager = () -> getInstance(Key.get(CacheManager.class));
    protected final Supplier<CachedTableMapping> cachedTableMapping = () -> getInstance(Key.get(CachedTableMapping.class));
    protected final Supplier<PgMetastore> pgMetastore = () -> getInstance(Key.get(PgMetastore.class));

    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        Path dir = Files.createTempDirectory(getWrenDirectory());
        if (getWrenMDLPath().isPresent()) {
            Files.copy(Path.of(getWrenMDLPath().get()), dir.resolve("mdl.json"));
        }
        else {
            Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(WrenMDL.EMPTY.getManifest()));
        }
        return TestingWrenServer.builder()
                .setRequiredConfigs(getProperties().put("wren.directory", dir.toString()).build())
                .build();
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
        WrenMDL mdl = getInstance(Key.get(WrenMetastore.class)).getAnalyzedMDL().getWrenMDL();
        for (CacheInfo cached : mdl.listCached()) {
            waitCacheReady(mdl.getCatalog(), mdl.getSchema(), cached.getName());
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
                .put("wren.datasource.type", "bigquery");

        return properties;
    }

    protected Optional<CacheInfoPair> getDefaultCacheInfoPair(String name)
    {
        return Optional.ofNullable(cachedTableMapping.get().getCacheInfoPair("wrenai", "tpch_tiny", name));
    }

    protected List<Object[]> queryDuckdb(String statement)
    {
        try (ConnectorRecordIterator iterator = pgMetastore.get().directQuery(statement, ImmutableList.of())) {
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

    private void waitCacheReady(String catalog, String schema, String name)
            throws Exception
    {
        CompletableFuture.runAsync(() -> {
            try {
                while (true) {
                    Optional<CacheInfoPair> cachedTable = Optional.ofNullable(cachedTableMapping.get().getCacheInfoPair(catalog, schema, name));
                    if (cachedTable.isPresent()) {
                        break;
                    }
                    Thread.sleep(1000);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).get(30, TimeUnit.SECONDS);
    }
}
