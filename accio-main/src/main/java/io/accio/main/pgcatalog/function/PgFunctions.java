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

package io.accio.main.pgcatalog.function;

import com.google.common.collect.ImmutableList;
import io.accio.base.type.RecordType;

import java.util.List;

import static io.accio.base.type.BigIntType.BIGINT;
import static io.accio.base.type.BooleanType.BOOLEAN;
import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.PGArray.INT4_ARRAY;
import static io.accio.base.type.PGArray.VARCHAR_ARRAY;
import static io.accio.base.type.TimestampType.TIMESTAMP;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.main.pgcatalog.function.PgFunction.Argument.argument;
import static io.accio.main.pgcatalog.function.PgFunction.Language.SQL;
import static io.accio.main.pgcatalog.function.PgFunction.builder;

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

    // TODO Support more date/time format https://www.postgresql.org/docs/13/functions-formatting.html#FUNCTIONS-FORMATTING-DATETIME-TABLE
    // TODO Support more timezone, now only support UTC
    public static final PgFunction PG_TO_CHAR = builder()
            .setName("to_char")
            .setLanguage(SQL)
            .setDefinition("WITH to_char AS (SELECT " +
                    "CONTAINS_SUBSTR(string_format, 'TZ') as contain_timezone, " +
                    "CAST(TIMESTAMP(value) AS STRING FORMAT REPLACE(REPLACE(string_format, 'MS', 'FF3'), 'TZ', '')) AS timestamp_with_format) " +
                    "SELECT CASE WHEN contain_timezone " +
                    "THEN CONCAT(timestamp_with_format, 'UTC') " +
                    "ELSE timestamp_with_format " +
                    "END " +
                    "FROM to_char")
            .setArguments(List.of(argument("value", TIMESTAMP), argument("string_format", VARCHAR)))
            .setReturnType(VARCHAR)
            .build();

    public static final PgFunction NOW = builder()
            .setName("now")
            .setLanguage(SQL)
            .setDefinition("SELECT CURRENT_TIMESTAMP")
            .setReturnType(TIMESTAMP)
            .build();

    // TODO This is a mock function, need to be implemented
    public static final PgFunction PG_EXPANDARRAY = builder()
            .setName("_pg_expandarray")
            .setLanguage(SQL)
            .setDefinition("SELECT CASE WHEN (array_length(int_arr) > 0) THEN CAST((int_arr[0], 1) AS STRUCT<x INT64, n INT64>) ELSE NULL END")
            .setArguments(List.of(argument("int_arr", INT4_ARRAY)))
            .setReturnType(new RecordType(List.of(BIGINT, BIGINT)))
            .build();
}
