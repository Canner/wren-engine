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

package io.accio.main.wireprotocol.patterns;

import java.util.regex.Pattern;

/*
 * There are too many issue to support getProcedureColumns for duckdb. DuckDB doesn't have procedure.
 * Let return empty result for now.
 */
public class JdbcGetProcedureColumnsPattern
        extends QueryPattern
{
    static final QueryPattern INSTANCE = new JdbcGetProcedureColumnsPattern();

    public JdbcGetProcedureColumnsPattern()
    {
        super(Pattern.compile("(?i)^ *SELECT n.nspname,p.proname,p.prorettype,p.proargtypes, t.typtype,t.typrelid,  p.proargnames, p.proargmodes, p.proallargtypes, p.oid  " +
                "FROM pg_catalog.pg_proc p, pg_catalog.pg_namespace n, pg_catalog.pg_type t  WHERE p.pronamespace=n.oid AND p.prorettype=t.oid  ORDER BY n.nspname, p.proname, " +
                "p.oid::text", Pattern.CASE_INSENSITIVE));
    }

    @Override
    protected String rewrite(String statement)
    {
        return "SELECT 1 limit 0";
    }
}
