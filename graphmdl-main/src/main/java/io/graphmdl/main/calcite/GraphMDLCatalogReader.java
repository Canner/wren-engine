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

package io.graphmdl.main.calcite;

import com.google.common.collect.ImmutableList;
import io.graphmdl.base.SessionContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;

public final class GraphMDLCatalogReader
        extends CalciteCatalogReader
{
    public GraphMDLCatalogReader(CalciteSchema rootSchema, SessionContext sessionContext, RelDataTypeFactory typeFactory, CalciteConnectionConfig config)
    {
        super(rootSchema, SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()), getSchemaPaths(sessionContext), typeFactory, config);
    }

    private static List<List<String>> getSchemaPaths(SessionContext sessionContext)
    {
        ImmutableList.Builder<List<String>> schemaPaths = ImmutableList.builder();

        // let catalog reader find table with full table name (i.e. {catalog}.{schema}.{table}) in sql.
        schemaPaths.add(List.of(""));

        // let catalog reader find table with table name with schema (i.e. {schema}.{table}) in sql.
        if (sessionContext.getCatalog().isPresent()) {
            schemaPaths.add(ImmutableList.of(sessionContext.getCatalog().get()));
        }

        // let catalog reader match table name with schema (i.e. {table}) in sql.
        if (sessionContext.getCatalog().isPresent() && sessionContext.getSchema().isPresent()) {
            schemaPaths.add(ImmutableList.of(sessionContext.getCatalog().get(), sessionContext.getSchema().get()));
        }

        return schemaPaths.build();
    }
}
