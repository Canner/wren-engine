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

package io.cml.type;

import com.google.common.collect.ImmutableMap;
import io.cml.spi.type.PGType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.cml.type.AnyType.ANY;
import static io.cml.type.BigIntType.BIGINT;
import static io.cml.type.BooleanType.BOOLEAN;
import static io.cml.type.BpCharType.BPCHAR;
import static io.cml.type.ByteaType.BYTEA;
import static io.cml.type.CharType.CHAR;
import static io.cml.type.DateType.DATE;
import static io.cml.type.DoubleType.DOUBLE;
import static io.cml.type.HstoreType.HSTORE;
import static io.cml.type.InetType.INET;
import static io.cml.type.IntegerType.INTEGER;
import static io.cml.type.JsonType.JSON;
import static io.cml.type.NumericType.NUMERIC;
import static io.cml.type.OidType.OID_INSTANCE;
import static io.cml.type.RealType.REAL;
import static io.cml.type.RecordType.EMPTY_RECORD;
import static io.cml.type.SmallIntType.SMALLINT;
import static io.cml.type.TimestampType.TIMESTAMP;
import static io.cml.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE;
import static io.cml.type.UuidType.UUID;
import static io.cml.type.VarcharType.NameType.NAME;
import static io.cml.type.VarcharType.TextType.TEXT;
import static io.cml.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public final class PGTypes
{
    private PGTypes() {}

    private static final Map<Integer, PGType<?>> TYPE_TABLE = new HashMap<>();
    private static final Set<PGType<?>> TYPES;

    static {
        TYPE_TABLE.put(BOOLEAN.oid(), BOOLEAN);
        TYPE_TABLE.put(SMALLINT.oid(), SMALLINT);
        TYPE_TABLE.put(INTEGER.oid(), INTEGER);
        TYPE_TABLE.put(BIGINT.oid(), BIGINT);
        TYPE_TABLE.put(REAL.oid(), REAL);
        TYPE_TABLE.put(DOUBLE.oid(), DOUBLE);
        TYPE_TABLE.put(NUMERIC.oid(), NUMERIC);
        TYPE_TABLE.put(VARCHAR.oid(), VARCHAR);
        TYPE_TABLE.put(CHAR.oid(), CHAR);
        TYPE_TABLE.put(JSON.oid(), JSON);
        TYPE_TABLE.put(TIMESTAMP.oid(), TIMESTAMP);
        TYPE_TABLE.put(TIMESTAMP_WITH_TIMEZONE.oid(), TIMESTAMP_WITH_TIMEZONE);
        TYPE_TABLE.put(TEXT.oid(), TEXT);
        TYPE_TABLE.put(NAME.oid(), NAME);
        TYPE_TABLE.put(OID_INSTANCE.oid(), OID_INSTANCE);
        TYPE_TABLE.put(DATE.oid(), DATE);
        TYPE_TABLE.put(BYTEA.oid(), BYTEA);
        TYPE_TABLE.put(BPCHAR.oid(), BPCHAR);
        // we handle all unspecified type as text type.
        TYPE_TABLE.put(0, TEXT);
        TYPE_TABLE.put(INET.oid(), INET);
        TYPE_TABLE.put(EMPTY_RECORD.oid(), EMPTY_RECORD);
        // Just need a fake instance to do type mapping. We never use the field.
        TYPE_TABLE.put(HSTORE.oid(), HSTORE);
        TYPE_TABLE.put(UUID.oid(), UUID);

        ImmutableMap.Builder<Integer, PGArray> innerToPgTypeBuilder = ImmutableMap.builder();
        // initial collection types
        PGArray.allArray().forEach(array -> {
            PGType<?> innerType = array.getInnerType();
            TYPE_TABLE.put(array.oid(), array);
            innerToPgTypeBuilder.put(innerType.oid(), array);
        });

        TYPES = new HashSet<>(TYPE_TABLE.values());
        // the following polymorphic types are added manually,
        // because there are no corresponding data types in Cannerflow
        TYPES.add(ANY);
    }

    public static Iterable<PGType<?>> pgTypes()
    {
        return TYPES;
    }

    public static PGType<?> oidToPgType(int oid)
    {
        PGType<?> pgType = TYPE_TABLE.get(oid);
        if (pgType == null) {
            throw new IllegalArgumentException(
                    format("No oid mapping from '%s' to pg_type", oid));
        }
        return pgType;
    }
}
