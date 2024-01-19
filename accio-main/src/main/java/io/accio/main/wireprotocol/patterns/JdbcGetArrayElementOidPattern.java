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

import io.accio.base.Parameter;
import io.accio.base.type.IntegerType;
import io.accio.base.type.PGType;
import io.accio.base.type.PGTypes;

import java.util.List;
import java.util.regex.Pattern;

import static java.lang.String.format;

/**
 * Because the oid in duckdb doesn't match the oid in postgresql, we need to rewrite the query to get the array element oid.
 * refer to https://github.com/pgjdbc/pgjdbc/blob/d91843a1c056ebe61343b1d3c0123bc42dcd7730/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L732
 */
public class JdbcGetArrayElementOidPattern
        extends QueryWithParamPattern
{
    private final PGType<?> type;

    protected JdbcGetArrayElementOidPattern(PGType<?> type)
    {
        super(Pattern.compile("SELECT e\\.oid, n\\.nspname = ANY\\(current_schemas\\(true\\)\\), n\\.nspname, e\\.typname FROM pg_catalog\\.pg_type t " +
                                "JOIN pg_catalog\\.pg_type e ON t\\.typelem = e\\.oid JOIN pg_catalog\\.pg_namespace n ON t\\.typnamespace = n\\.oid WHERE t\\.oid = \\$1",
                        Pattern.CASE_INSENSITIVE),
                List.of(new Parameter(IntegerType.INTEGER, type.oid())));
        this.type = type;
    }

    @Override
    protected String rewrite(String statement)
    {
        PGType<?> elemType = PGTypes.oidToPgType(type.typElem());
        return format("SELECT %s oid, true, 'pg_catalog', '%s'", elemType.oid(), elemType.typName());
    }
}
