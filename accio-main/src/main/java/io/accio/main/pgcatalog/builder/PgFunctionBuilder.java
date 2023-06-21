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

package io.accio.main.pgcatalog.builder;

import io.accio.main.metadata.Metadata;
import io.accio.main.pgcatalog.function.PgFunction;
import io.airlift.log.Logger;

public abstract class PgFunctionBuilder
{
    private static final Logger LOG = Logger.get(PgFunctionBuilder.class);
    private final Metadata connector;

    public PgFunctionBuilder(Metadata connector)
    {
        this.connector = connector;
    }

    public void createPgFunction(PgFunction pgFunction)
    {
        String sql = generateCreateFunction(pgFunction);
        connector.directDDL(sql);
        LOG.info("pg_catalog.%s has created or updated", pgFunction.getName());
    }

    protected abstract String generateCreateFunction(PgFunction pgFunction);
}
