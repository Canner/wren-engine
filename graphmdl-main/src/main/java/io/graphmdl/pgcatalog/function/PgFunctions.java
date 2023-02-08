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

package io.graphmdl.pgcatalog.function;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static io.graphmdl.pgcatalog.function.PgFunction.Argument.argument;
import static io.graphmdl.pgcatalog.function.PgFunction.Language.SQL;
import static io.graphmdl.pgcatalog.function.PgFunction.builder;
import static io.graphmdl.spi.type.BigIntType.BIGINT;
import static io.graphmdl.spi.type.BooleanType.BOOLEAN;
import static io.graphmdl.spi.type.IntegerType.INTEGER;
import static io.graphmdl.spi.type.PGArray.VARCHAR_ARRAY;
import static io.graphmdl.spi.type.VarcharType.VARCHAR;

public final class PgFunctions
{
    private static final String EMPTY_STATEMENT = "SELECT null LIMIT 0";

    private PgFunctions() {}

    public static final PgFunction CURRENT_DATABASE = builder()
            .setName("current_database")
            .setLanguage(SQL)
            .setDefinition("SELECT DISTINCT catalog_name FROM INFORMATION_SCHEMA.SCHEMATA")
            .setReturnType(VARCHAR)
            .build();

    // TODO: consider region
    // https://github.com/Canner/canner-metric-layer/issues/64
    public static final PgFunction CURRENT_SCHEMAS = builder()
            .setName("current_schemas")
            .setLanguage(SQL)
            .setDefinition("SELECT ARRAY(SELECT DISTINCT schema_name FROM INFORMATION_SCHEMA.SCHEMATA)")
            .setArguments(ImmutableList.of(argument("include_implicit", BOOLEAN)))
            .setReturnType(VARCHAR_ARRAY)
            .build();

    public static final PgFunction PG_RELATION_SIZE__INT___BIGINT = builder()
            .setName("pg_relation_size")
            .setLanguage(SQL)
            .setDefinition(EMPTY_STATEMENT)
            .setArguments(ImmutableList.of(argument("relOid", INTEGER)))
            .setReturnType(BIGINT)
            .build();

    public static final PgFunction PG_RELATION_SIZE__INT_VARCHAR___BIGINT = builder()
            .setName("pg_relation_size")
            .setLanguage(SQL)
            .setDefinition(EMPTY_STATEMENT)
            .setArguments(ImmutableList.of(argument("relOid", INTEGER), argument("text", VARCHAR)))
            .setReturnType(BIGINT)
            .build();

    public static final PgFunction ARRAY_IN = builder()
            .setName("array_in")
            .setLanguage(SQL)
            .setDefinition(EMPTY_STATEMENT)
            .setArguments(ImmutableList.of(argument("ignored", VARCHAR)))
            .setReturnType(VARCHAR_ARRAY)
            .build();

    public static final PgFunction ARRAY_OUT = builder()
            .setName("array_out")
            .setLanguage(SQL)
            .setDefinition(EMPTY_STATEMENT)
            .setArguments(ImmutableList.of(argument("ignored", VARCHAR_ARRAY)))
            .setReturnType(VARCHAR)
            .build();

    public static final PgFunction ARRAY_RECV = builder()
            .setName("array_recv")
            .setLanguage(SQL)
            .setDefinition(EMPTY_STATEMENT)
            .setArguments(ImmutableList.of(argument("ignored", VARCHAR)))
            .setReturnType(VARCHAR_ARRAY)
            .build();

    // BigQuery didn't support nested array now.
    // Only handle 1-dim array here.
    public static final PgFunction ARRAY_UPPER = builder()
            .setName("array_upper")
            .setLanguage(SQL)
            .setDefinition("SELECT CASE WHEN dim = 1 THEN array_length(input) ELSE NULL END")
            .setArguments(ImmutableList.of(argument("input", VARCHAR_ARRAY), argument("dim", BIGINT)))
            .setReturnType(INTEGER)
            .build();

    public static final PgFunction PG_GET_EXPR = builder()
            .setName("pg_get_expr")
            .setLanguage(SQL)
            .setDefinition("SELECT ''")
            .setArguments(List.of(argument("pg_node", VARCHAR), argument("relation", INTEGER)))
            .setReturnType(VARCHAR)
            .build();

    public static final PgFunction PG_GET_EXPR_PRETTY = builder()
            .setName("pg_get_expr")
            .setLanguage(SQL)
            .setDefinition("SELECT ''")
            .setArguments(List.of(argument("pg_node", VARCHAR), argument("relation", INTEGER), argument("pretty", BOOLEAN)))
            .setReturnType(VARCHAR)
            .build();

    public static final PgFunction FORMAT_TYPE = builder()
            .setName("format_type")
            .setLanguage(SQL)
            .setDefinition("SELECT CASE WHEN type IS NULL THEN null ELSE CASE WHEN ARRAY_LENGTH(typresult) = 0 THEN '???' ELSE typresult[ordinal(1)] END END " +
                    "FROM (SELECT array(SELECT typname FROM pg_catalog.pg_type WHERE oid = type) as typresult)")
            .setArguments(List.of(argument("type", INTEGER), argument("typmod", INTEGER)))
            .setReturnType(VARCHAR)
            .build();

    public static final PgFunction PG_GET_FUNCTION_RESULT = builder()
            .setName("pg_get_function_result")
            .setLanguage(SQL)
            .setDefinition("select regexp_extract(remotename, r\".*___([_a-zA-Z1-9]+)*\", 1) from pg_catalog.pg_proc WHERE oid = func")
            .setArguments(List.of(argument("func", INTEGER)))
            .setReturnType(VARCHAR)
            .build();
}
