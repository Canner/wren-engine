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

package io.graphmdl.connector.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableMap;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.type.DateType;
import io.graphmdl.base.type.IntervalType;
import io.graphmdl.base.type.JsonType;
import io.graphmdl.base.type.NumericType;
import io.graphmdl.base.type.PGArray;
import io.graphmdl.base.type.PGType;
import io.graphmdl.base.type.RecordType;
import io.graphmdl.base.type.TimestampType;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.cloud.bigquery.StandardSQLTypeName.ARRAY;
import static com.google.cloud.bigquery.StandardSQLTypeName.BIGNUMERIC;
import static com.google.cloud.bigquery.StandardSQLTypeName.BOOL;
import static com.google.cloud.bigquery.StandardSQLTypeName.BYTES;
import static com.google.cloud.bigquery.StandardSQLTypeName.DATE;
import static com.google.cloud.bigquery.StandardSQLTypeName.DATETIME;
import static com.google.cloud.bigquery.StandardSQLTypeName.FLOAT64;
import static com.google.cloud.bigquery.StandardSQLTypeName.INT64;
import static com.google.cloud.bigquery.StandardSQLTypeName.INTERVAL;
import static com.google.cloud.bigquery.StandardSQLTypeName.JSON;
import static com.google.cloud.bigquery.StandardSQLTypeName.NUMERIC;
import static com.google.cloud.bigquery.StandardSQLTypeName.STRING;
import static com.google.cloud.bigquery.StandardSQLTypeName.STRUCT;
import static com.google.cloud.bigquery.StandardSQLTypeName.TIMESTAMP;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.graphmdl.base.metadata.StandardErrorCode.NOT_SUPPORTED;
import static io.graphmdl.base.type.BigIntType.BIGINT;
import static io.graphmdl.base.type.BooleanType.BOOLEAN;
import static io.graphmdl.base.type.ByteaType.BYTEA;
import static io.graphmdl.base.type.CharType.CHAR;
import static io.graphmdl.base.type.DoubleType.DOUBLE;
import static io.graphmdl.base.type.IntegerType.INTEGER;
import static io.graphmdl.base.type.RealType.REAL;
import static io.graphmdl.base.type.RecordType.EMPTY_RECORD;
import static io.graphmdl.base.type.SmallIntType.SMALLINT;
import static io.graphmdl.base.type.TinyIntType.TINYINT;
import static io.graphmdl.base.type.VarcharType.VARCHAR;

public final class BigQueryType
{
    private BigQueryType() {}

    private static final Map<StandardSQLTypeName, PGType<?>> bqTypeToPgTypeMap;
    private static final Map<PGType<?>, StandardSQLTypeName> pgTypeToBqTypeMap;

    static {
        bqTypeToPgTypeMap = ImmutableMap.<StandardSQLTypeName, PGType<?>>builder()
                .put(BOOL, BOOLEAN)
                .put(INT64, BIGINT)
                .put(STRING, VARCHAR)
                .put(FLOAT64, DOUBLE)
                .put(DATE, DateType.DATE)
                .put(BYTES, BYTEA)
                .put(DATETIME, TimestampType.TIMESTAMP)
                .put(TIMESTAMP, TimestampType.TIMESTAMP)
                .put(NUMERIC, NumericType.NUMERIC)
                .put(BIGNUMERIC, NumericType.NUMERIC)
                .put(JSON, VARCHAR)
                .put(INTERVAL, IntervalType.INTERVAL)
                .build();

        pgTypeToBqTypeMap = ImmutableMap.<PGType<?>, StandardSQLTypeName>builder()
                .put(BOOLEAN, BOOL)
                .put(BIGINT, INT64)
                .put(INTEGER, INT64)
                .put(SMALLINT, INT64)
                .put(TINYINT, INT64)
                .put(CHAR, STRING)
                .put(VARCHAR, STRING)
                .put(DOUBLE, FLOAT64)
                .put(REAL, FLOAT64)
                .put(NumericType.NUMERIC, BIGNUMERIC)
                .put(DateType.DATE, DATE)
                .put(TimestampType.TIMESTAMP, TIMESTAMP)
                .put(BYTEA, BYTES)
                .put(IntervalType.INTERVAL, INTERVAL)
                .put(JsonType.JSON, JSON)
                .build();
    }

    public static PGType<?> toPGType(Field field)
    {
        StandardSQLTypeName bigQueryType = field.getType().getStandardType();
        if (bigQueryType.equals(STRUCT)) {
            if (field.getSubFields().isEmpty()) {
                return EMPTY_RECORD;
            }
            List<PGType<?>> innerPgTypes = field.getSubFields().stream().map(BigQueryType::toPGType).collect(toImmutableList());
            return new RecordType(innerPgTypes);
        }
        return toPGType(bigQueryType);
    }

    private static PGType<?> toPGType(StandardSQLTypeName bigQueryType)
    {
        return Optional.ofNullable(bqTypeToPgTypeMap.get(bigQueryType))
                .orElseThrow(() -> new GraphMDLException(NOT_SUPPORTED, "Unsupported Type: " + bigQueryType));
    }

    public static StandardSQLTypeName toBqType(PGType<?> pgType)
    {
        if (pgType instanceof PGArray) {
            return ARRAY;
        }
        return Optional.ofNullable(pgTypeToBqTypeMap.get(pgType))
                .orElseThrow(() -> new GraphMDLException(NOT_SUPPORTED, "Unsupported Type: " + pgType.typName()));
    }

    public static Object toBqValue(PGType<?> pgType, Object value)
    {
        if (pgType instanceof PGArray && value instanceof List) {
            PGType<?> innerType = ((PGArray) pgType).getInnerType();
            return ((List<Object>) value).stream()
                    .map(v -> toBqValue(innerType, v))
                    .toArray();
        }

        // Cast a short type value to an integer to fit the BigQuery type INT64, because there is the limitation of BigQuery API in QueryParameterValue.
        // https://github.com/googleapis/java-bigquery/blob/909a574e6857332dfc71c746c4500b601de57dcf/google-cloud-bigquery/src/main/java/com/google/cloud/bigquery/QueryParameterValue.java#L409
        if (pgType.equals(SMALLINT) && value instanceof Short) {
            return ((Short) value).intValue();
        }
        if (pgType.equals(IntervalType.INTERVAL) && value instanceof Period) {
            return value.toString();
        }
        return value;
    }
}
