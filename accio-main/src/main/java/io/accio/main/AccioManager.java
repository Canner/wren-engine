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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.accio.base.AccioMDL;
import io.accio.cache.CacheManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;

import static io.accio.base.AccioMDL.EMPTY;
import static java.util.Objects.requireNonNull;

public class AccioManager
        implements AccioMetastore
{
    private static final Logger LOG = Logger.get(AccioManager.class);
    private final AtomicReference<AccioMDL> accioMDL = new AtomicReference<>(EMPTY);
    private final File accioMDLFile;
    private final CacheManager cacheManager;

    @Inject
    public AccioManager(AccioConfig accioConfig, CacheManager cacheManager)
            throws IOException
    {
        this.accioMDLFile = requireNonNull(accioConfig.getAccioMDLFile(), "accioMDLFile is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        if (accioMDLFile.exists()) {
            loadAccioMDLFromFile();
            cacheManager.createTaskUtilDone(getAccioMDL());
        }
        else {
            LOG.warn("AccioMDL file %s does not exist", accioMDLFile);
        }
    }

    public synchronized void loadAccioMDLFromFile()
            throws IOException
    {
        loadAccioMDL(Files.readString(accioMDLFile.toPath()));
    }

    private void loadAccioMDL(String json)
            throws JsonProcessingException
    {
        AccioMDL oldAccioMDL = accioMDL.get();
        cacheManager.removeCache(oldAccioMDL.getCatalog(), oldAccioMDL.getSchema());
        accioMDL.set(AccioMDL.fromJson(json));
    }

    @Override
    public AccioMDL getAccioMDL()
    {
        return accioMDL.get();
    }
}
