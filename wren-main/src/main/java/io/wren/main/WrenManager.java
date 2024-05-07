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

package io.wren.main;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.wren.base.AnalyzedMDL;
import io.wren.base.WrenException;
import io.wren.base.WrenMDL;
import io.wren.base.config.WrenConfig;
import io.wren.base.dto.Manifest;
import io.wren.cache.CacheManager;
import io.wren.main.pgcatalog.PgCatalogManager;
import io.wren.main.pgcatalog.exception.DeployException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.wren.base.Utils.checkArgument;
import static io.wren.base.client.duckdb.FileUtil.ARCHIVED;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.wren.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.wren.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class WrenManager
{
    private static final Logger LOG = Logger.get(WrenManager.class);
    private File wrenMDLFile;
    private final File wrenMDLDirectory;
    private final CacheManager cacheManager;
    private final PgCatalogManager pgCatalogManager;
    private final WrenMetastore wrenMetastore;

    @Inject
    public WrenManager(WrenConfig wrenConfig,
            WrenMetastore wrenMetastore,
            CacheManager cacheManager,
            PgCatalogManager pgCatalogManager)
    {
        requireNonNull(wrenConfig, "wrenConfig is null");
        this.wrenMDLDirectory = requireNonNull(wrenConfig.getWrenMDLDirectory(), "wrenMDLDirectory is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.pgCatalogManager = requireNonNull(pgCatalogManager, "pgCatalogManager is null");
        this.wrenMetastore = requireNonNull(wrenMetastore, "wrenMetastore is null");
        File[] mdlFiles = wrenMDLDirectory.listFiles((dir, name) -> name.endsWith(".json"));
        try {
            if (mdlFiles != null && mdlFiles.length > 0) {
                deployWrenMDLFromDir(mdlFiles);
            }
            else {
                LOG.warn("No WrenMDL file found. WrenMDL will not be deployed, and no pg table will be generated.");
            }
        }
        catch (IOException e) {
            LOG.warn("Load WrenMDL file failed. WrenMDL will not be deployed, and no pg table will be generated.", e);
        }
        catch (Exception e) {
            LOG.error(e, "Failed to deploy WrenMDL");
        }
    }

    private void deployWrenMDLFromDir(File[] mdlFiles)
            throws IOException, DeployException
    {
        List<File> mdls = Arrays.stream(mdlFiles)
                .filter(file -> file.getName().endsWith(".json"))
                .collect(toList());
        checkArgument(mdls.size() == 1, "There should be only one mdl file in the directory");
        wrenMDLFile = mdls.get(0);
        File versionFile = wrenMDLDirectory.toPath().resolve("version").toFile();
        String version = versionFile.exists() ? Files.readString(versionFile.toPath()) : null;
        deployWrenMDLFromFile(version);
    }

    private void deployWrenMDLFromFile(String version)
            throws IOException, DeployException
    {
        String json = Files.readString(wrenMDLFile.toPath());
        wrenMetastore.setWrenMDL(WrenMDL.fromManifest(MANIFEST_JSON_CODEC.fromJson(json)), version);
        deploy();
    }

    public synchronized void deployAndArchive(Manifest manifest, String version)
    {
        checkArgument(wrenMDLDirectory.exists() &&
                wrenMDLDirectory.isDirectory() &&
                requireNonNull(wrenMDLDirectory.listFiles()).length > 0, "WrenMDL directory does not exist or is empty");
        try {
            Files.write(wrenMDLDirectory.toPath().resolve("version"), Optional.ofNullable(version).map(v -> v.getBytes(UTF_8)).orElse(new byte[0]));
            WrenMDL oldWrenMDL = wrenMetastore.getAnalyzedMDL().getWrenMDL();
            wrenMetastore.setWrenMDL(WrenMDL.fromManifest(manifest), version);
            archiveWrenMDL(oldWrenMDL);
            Files.write(wrenMDLFile.toPath(), MANIFEST_JSON_CODEC.toJson(wrenMetastore.getAnalyzedMDL().getWrenMDL().getManifest()).getBytes(UTF_8));
            // pre drop if the schema name is changed.
            pgCatalogManager.dropSchema(oldWrenMDL.getSchema());
            deploy();
        }
        catch (IOException e) {
            LOG.error(e, "Failed to archive WrenMDL file");
            throw new WrenException(GENERIC_INTERNAL_ERROR, e);
        }
        catch (DeployException e) {
            LOG.error(e, "Failed to deploy WrenMDL");
            throw new WrenException(GENERIC_USER_ERROR, e);
        }
    }

    private void deploy()
            throws DeployException
    {
        cacheManager.createTask(getAnalyzedMDL());
        pgCatalogManager.initPgCatalog();
    }

    private void archiveWrenMDL(WrenMDL oldWrenMDL)
            throws IOException
    {
        cacheManager.removeCacheIfExist(oldWrenMDL.getCatalog(), oldWrenMDL.getSchema());
        File archived = new File(wrenMDLDirectory.getAbsoluteFile() + "/" + ARCHIVED);
        if (!archived.exists()) {
            if (!archived.mkdir()) {
                throw new IOException("Cannot create archive folder");
            }
        }
        Files.copy(wrenMDLFile.toPath(),
                archived.toPath().resolve(wrenMDLFile.getName() + "." + LocalDateTime.now().format(DateTimeFormatter.ofPattern("uuuuMMddHHmmssnnnn"))));
    }

    public boolean checkStatus()
    {
        return pgCatalogManager.checkRequired();
    }

    public AnalyzedMDL getAnalyzedMDL()
    {
        return wrenMetastore.getAnalyzedMDL();
    }
}
