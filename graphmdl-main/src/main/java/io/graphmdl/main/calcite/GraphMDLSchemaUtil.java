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

import org.apache.calcite.sql.SqlDialect;

import static io.graphmdl.main.calcite.BigQueryGraphMDLSqlDialect.DEFAULT_CONTEXT;

public final class GraphMDLSchemaUtil
{
    private GraphMDLSchemaUtil() {}

    public enum Dialect
    {
        BIGQUERY(new BigQueryGraphMDLSqlDialect(DEFAULT_CONTEXT));

        private final SqlDialect sqlDialect;

        Dialect(SqlDialect sqlDialect)
        {
            this.sqlDialect = sqlDialect;
        }

        public SqlDialect getSqlDialect()
        {
            return sqlDialect;
        }
    }
}
