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

package io.wren.main.pgcatalog.builder;

import io.airlift.log.Logger;
import io.wren.base.pgcatalog.function.PgFunction;
import io.wren.main.metadata.Metadata;
import io.wren.main.metadata.MetadataManager;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PgFunctionBuilderManager
{
    private static final Logger LOG = Logger.get(PgFunctionBuilderManager.class);
    private final MetadataManager metadataManager;

    @Inject
    public PgFunctionBuilderManager(
            Metadata metadataManager)
    {
        this.metadataManager = (MetadataManager) requireNonNull(metadataManager, "metadataManager is null");
    }

    public void createPgFunction(PgFunction pgFunction)
    {
        String sql = metadataManager.getPgFunctionBuilder().generateCreateFunction(pgFunction);
        LOG.info("Creating or updating %s.%s: %s", metadataManager.getPgCatalogName(), pgFunction.getName(), sql);
        metadataManager.directDDL(sql);
        LOG.info("%s.%s has created or updated", metadataManager.getPgCatalogName(), pgFunction.getName());
    }
}
