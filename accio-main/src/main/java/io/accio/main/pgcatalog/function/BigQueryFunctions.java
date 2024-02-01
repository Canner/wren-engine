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

import io.accio.base.type.RecordType;

import java.util.List;

import static io.accio.base.type.AnyType.ANY;
import static io.accio.base.type.BigIntType.BIGINT;
import static io.accio.base.type.PGArray.INT4_ARRAY;
import static io.accio.base.type.TimestampType.TIMESTAMP;
import static io.accio.base.type.VarcharType.VARCHAR;
import static io.accio.main.pgcatalog.function.PgFunction.Argument.argument;
import static io.accio.main.pgcatalog.function.PgFunction.Language.SQL;
import static io.accio.main.pgcatalog.function.PgFunction.builder;

public final class BigQueryFunctions
{
    private BigQueryFunctions() {}

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
            .setSubquery(true)
            .setArguments(List.of(argument("value", TIMESTAMP), argument("string_format", VARCHAR)))
            .setReturnType(VARCHAR)
            .build();

    public static final PgFunction NOW = builder()
            .setName("now")
            .setLanguage(SQL)
            .setDefinition("SELECT CURRENT_DATETIME")
            .setReturnType(TIMESTAMP)
            .build();

    // TODO This is a mock function, need to be implemented
    public static final PgFunction PG_EXPANDARRAY = builder()
            .setName("_pg_expandarray")
            .setLanguage(SQL)
            .setDefinition("CASE WHEN (array_length(int_arr) > 0) THEN cast((int_arr[0], 1) as row(x int, n int)) ELSE NULL END")
            .setArguments(List.of(argument("int_arr", INT4_ARRAY)))
            .setReturnType(new RecordType(List.of(BIGINT, BIGINT)))
            .build();

    // TODO If the input is a string only include number, it will be parsed as a number. So substring('123' from '1') would get the wrong answer '123', actual should be '1'
    // https://github.com/Canner/accio/issues/329
    public static final PgFunction SUBSTR = builder()
            .setName("substr")
            .setLanguage(SQL)
            .setDefinition("SELECT " +
                    "CASE WHEN REGEXP_CONTAINS(SAFE_CAST(arg2 AS STRING), '^[0-9]*$') IS TRUE\n" +
                    "THEN SUBSTR(arg1, CAST(arg2 AS INT64))\n" +
                    "ELSE REGEXP_EXTRACT(arg1, CAST(arg2 AS STRING))\n" +
                    "END")
            .setArguments(List.of(argument("arg1", VARCHAR), argument("arg2", ANY)))
            .setReturnType(VARCHAR)
            .build();
}
