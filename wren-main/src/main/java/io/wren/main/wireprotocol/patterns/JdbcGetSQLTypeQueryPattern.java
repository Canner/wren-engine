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

package io.wren.main.wireprotocol.patterns;

import io.wren.base.Parameter;
import io.wren.base.type.BigIntType;
import io.wren.base.type.PGType;

import java.util.List;
import java.util.regex.Pattern;

import static java.lang.String.format;

/**
 * Because the oid in duckdb doesn't match the oid in postgresql, we need to rewrite the query to get the type information.
 * Refer to https://github.com/pgjdbc/pgjdbc/blob/d91843a1c056ebe61343b1d3c0123bc42dcd7730/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L241
 */
public class JdbcGetSQLTypeQueryPattern
        extends QueryWithParamPattern
{
    private final PGType<?> type;

    protected JdbcGetSQLTypeQueryPattern(PGType<?> type)
    {
        super(Pattern.compile("(?i)^ *SELECT typinput='array_in'::regproc as is_array, typtype, typname, pg_type\\.oid   FROM pg_catalog\\.pg_type   LEFT JOIN \\(select ns\\.oid as " +
                                "nspoid, ns\\.nspname, r\\.r +from pg_namespace as ns +join \\( select s\\.r, \\(current_schemas\\(false\\)\\)\\[s\\.r] as nspname +" +
                                "from generate_series\\(1, array_upper\\(current_schemas\\(false\\), 1\\)\\) as s\\(r\\) \\) as r +using \\( nspname \\) *\\) as sp +ON " +
                                "sp\\.nspoid = typnamespace +WHERE pg_type\\.oid = \\$1 +ORDER BY sp\\.r, pg_type\\.oid DESC",
                        Pattern.CASE_INSENSITIVE),
                List.of(new Parameter(BigIntType.BIGINT, (long) type.oid())));
        this.type = type;
    }

    @Override
    protected String rewrite(String statement)
    {
        return format("SELECT %s is_array, '%s' typtype, '%s' typname, %s oid",
                type.typInput().equals("array_in") ? "true" : "false",
                type.typeCategory(),
                type.typName(),
                type.oid());
    }
}
