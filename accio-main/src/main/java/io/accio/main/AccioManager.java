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

package io.accio.main;

import io.accio.base.AccioException;
import io.accio.base.AccioMDL;
import io.accio.base.dto.Manifest;
import io.accio.cache.CacheManager;
import io.accio.main.pgcatalog.PgCatalogManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import static io.accio.base.Utils.checkArgument;
import static io.accio.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.accio.base.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AccioManager
{
    private static final Logger LOG = Logger.get(AccioManager.class);
    private File accioMDLFile;
    private final File accioMDLDirectory;
    private final CacheManager cacheManager;
    private final PgCatalogManager pgCatalogManager;
    private final AccioMetastore accioMetastore;
    private final AccioConfig accioConfig;

    @Inject
    public AccioManager(AccioConfig accioConfig,
            AccioMetastore accioMetastore,
            CacheManager cacheManager,
            PgCatalogManager pgCatalogManager)
            throws IOException
    {
        this.accioConfig = requireNonNull(accioConfig, "accioConfig is null");
        this.accioMDLDirectory = requireNonNull(accioConfig.getAccioMDLDirectory(), "accioMDLDirectory is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.pgCatalogManager = requireNonNull(pgCatalogManager, "pgCatalogManager is null");
        this.accioMetastore = requireNonNull(accioMetastore, "accioMetastore is null");
        if (accioMDLDirectory.exists() &&
                accioMDLDirectory.isDirectory() &&
                requireNonNull(accioMDLDirectory.listFiles()).length > 0) {
            deployAccioMDLFromDir();
        }
        else if (accioConfig.getAccioMDLFile().isPresent()) {
            this.accioMDLFile = accioConfig.getAccioMDLFile().get();
            deployAccioMDLFromFile();
        }
        else {
            LOG.warn("No AccioMDL file found. AccioMDL will not be deployed.");
        }
    }

    private void deployAccioMDLFromDir()
            throws IOException
    {
        List<File> mdls = Arrays.stream(requireNonNull(accioMDLDirectory.listFiles()))
                .filter(file -> file.getName().endsWith(".json"))
                .collect(toList());
        checkArgument(mdls.size() == 1, "There should be only one mdl file in the directory");
        accioMDLFile = mdls.get(0);
        deployAccioMDLFromFile();
    }

    private void deployAccioMDLFromFile()
            throws IOException
    {
        String json = Files.readString(accioMDLFile.toPath());
        accioMetastore.setAccioMDL(AccioMDL.fromManifest(MANIFEST_JSON_CODEC.fromJson(json)));
        deploy();
    }

    public synchronized void deployAndArchive(Manifest manifest)
    {
        checkArgument(accioConfig.getAccioMDLFile().isEmpty(), "Deprecated config `accio.file`. Please use `accio.directory` instead.");
        checkArgument(accioMDLDirectory.exists() &&
                accioMDLDirectory.isDirectory() &&
                requireNonNull(accioMDLDirectory.listFiles()).length > 0, "AccioMDL directory does not exist or is empty");
        try {
            AccioMDL oldAccioMDL = accioMetastore.getAccioMDL();
            accioMetastore.setAccioMDL(AccioMDL.fromManifest(manifest));
            archiveAccioMDL(oldAccioMDL);
            Files.write(accioMDLFile.toPath(), MANIFEST_JSON_CODEC.toJson(accioMetastore.getAccioMDL().getManifest()).getBytes(UTF_8));
            deploy();
        }
        catch (IOException e) {
            LOG.error(e, "Failed to archive AccioMDL file");
            throw new AccioException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private void deploy()
    {
        cacheManager.createTask(getAccioMDL());
        pgCatalogManager.initPgCatalog();
    }

    private void archiveAccioMDL(AccioMDL oldAccioMDL)
            throws IOException
    {
        cacheManager.removeCacheIfExist(oldAccioMDL.getCatalog(), oldAccioMDL.getSchema());
        File archived = new File(accioMDLDirectory.getAbsoluteFile() + "/archive");
        if (!archived.exists()) {
            if (!archived.mkdir()) {
                throw new IOException("Cannot create archive folder");
            }
        }
        Files.move(accioMDLFile.toPath(),
                archived.toPath().resolve(accioMDLFile.getName() + "." + LocalDateTime.now().format(DateTimeFormatter.ofPattern("uuuuMMddHHmmss"))));
    }

    public boolean checkStatus()
    {
        return pgCatalogManager.checkRequired();
    }

    public AccioMDL getAccioMDL()
    {
        return accioMetastore.getAccioMDL();
    }
}
