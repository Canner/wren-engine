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
package io.accio.main.connector.duckdb;

import io.accio.cache.CacheService;
import io.accio.cache.PathInfo;
import io.accio.main.metadata.Metadata;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DuckDBCacheService
        implements CacheService
{
    private final Metadata metadata;
    private final Path cacheFolder;

    @Inject
    public DuckDBCacheService(
            Metadata metadata)
            throws IOException
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.cacheFolder = Files.createTempDirectory("accio-duckdb-cache");
    }

    @Override
    public Optional<PathInfo> createCache(String catalog, String schema, String name, String statement)
    {
        String fileName = name + ".parquet";
        String path = format("%s/%s/%s",
                cacheFolder,
                catalog,
                schema);
        try {
            Path dir = Path.of(path);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        String exportStatement = format("COPY (%s) TO '%s/%s' (FORMAT PARQUET);",
                statement,
                path,
                fileName);
        metadata.directDDL(exportStatement);
        return Optional.of(PathInfo.of(path, fileName));
    }

    @Override
    public void deleteTarget(PathInfo pathInfo)
    {
        try {
            Files.deleteIfExists(Path.of(pathInfo.getPath(), pathInfo.getFilePattern()));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
