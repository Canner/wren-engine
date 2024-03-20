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
import io.wren.base.wireprotocol.PgMetastore;

public class PgMetastoreFunctionBuilder
        extends DuckDBFunctionBuilder
{
    private static final Logger LOG = Logger.get(PgMetastoreFunctionBuilder.class);

    private final PgMetastore pgMetastore;

    public PgMetastoreFunctionBuilder(PgMetastore pgMetastore)
    {
        super();
        this.pgMetastore = pgMetastore;
    }

    public void createPgFunction(PgFunction pgFunction)
    {
        String sql = generateCreateFunction(pgFunction);
        LOG.info("Creating or updating pg_catalog.%s: %s", pgFunction.getName(), sql);
        pgMetastore.directDDL(sql);
        LOG.info("pg_catalog.%s has created or updated", pgFunction.getName());
    }
}
