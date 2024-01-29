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

package io.accio.base.type;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.accio.base.type.BigIntType.BIGINT;
import static io.accio.base.type.BooleanType.BOOLEAN;
import static io.accio.base.type.CharType.CHAR;
import static io.accio.base.type.DateType.DATE;
import static io.accio.base.type.DoubleType.DOUBLE;
import static io.accio.base.type.HstoreType.HSTORE;
import static io.accio.base.type.IntegerType.INTEGER;
import static io.accio.base.type.IntervalType.INTERVAL;
import static io.accio.base.type.RealType.REAL;
import static io.accio.base.type.SmallIntType.SMALLINT;
import static io.accio.base.type.TimestampType.TIMESTAMP;
import static io.accio.base.type.VarcharType.NameType.NAME;
import static io.accio.base.type.VarcharType.TextType.TEXT;
import static io.accio.base.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public final class PGTypes
{
    private PGTypes() {}

    private static final Map<Integer, PGType<?>> TYPE_TABLE = new HashMap<>();
    private static final Map<Integer, PGArray> INNER_TYPE_TO_ARRAY_TABLE;
    private static final Set<PGType<?>> TYPES;

    private static final Map<String, PGType<?>> TYPE_NAME_TABLE = new HashMap<>();

    static {
        TYPE_TABLE.put(BOOLEAN.oid(), BOOLEAN);
        TYPE_TABLE.put(SMALLINT.oid(), SMALLINT);
        TYPE_TABLE.put(INTEGER.oid(), INTEGER);
        TYPE_TABLE.put(BIGINT.oid(), BIGINT);
        TYPE_TABLE.put(REAL.oid(), REAL);
        TYPE_TABLE.put(DOUBLE.oid(), DOUBLE);
        TYPE_TABLE.put(NumericType.NUMERIC.oid(), NumericType.NUMERIC);
        TYPE_TABLE.put(VARCHAR.oid(), VARCHAR);
        TYPE_TABLE.put(CHAR.oid(), CHAR);
        TYPE_TABLE.put(JsonType.JSON.oid(), JsonType.JSON);
        TYPE_TABLE.put(TIMESTAMP.oid(), TIMESTAMP);
        TYPE_TABLE.put(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE.oid(), TimestampWithTimeZoneType.TIMESTAMP_WITH_TIMEZONE);
        TYPE_TABLE.put(TEXT.oid(), TEXT);
        TYPE_TABLE.put(NAME.oid(), NAME);
        TYPE_TABLE.put(OidType.OID_INSTANCE.oid(), OidType.OID_INSTANCE);
        TYPE_TABLE.put(DATE.oid(), DATE);
        TYPE_TABLE.put(ByteaType.BYTEA.oid(), ByteaType.BYTEA);
        TYPE_TABLE.put(BpCharType.BPCHAR.oid(), BpCharType.BPCHAR);
        // we handle all unspecified type as text type.
        TYPE_TABLE.put(0, TEXT);
        TYPE_TABLE.put(InetType.INET.oid(), InetType.INET);
        TYPE_TABLE.put(RecordType.EMPTY_RECORD.oid(), RecordType.EMPTY_RECORD);
        // Just need a fake instance to do type mapping. We never use the field.
        TYPE_TABLE.put(HSTORE.oid(), HSTORE);
        TYPE_TABLE.put(UuidType.UUID.oid(), UuidType.UUID);
        TYPE_TABLE.put(INTERVAL.oid(), INTERVAL);

        ImmutableMap.Builder<Integer, PGArray> innerToPgTypeBuilder = ImmutableMap.builder();
        // initial collection types
        PGArray.allArray().forEach(array -> {
            PGType<?> innerType = array.getInnerType();
            TYPE_TABLE.put(array.oid(), array);
            innerToPgTypeBuilder.put(innerType.oid(), array);
        });

        INNER_TYPE_TO_ARRAY_TABLE = innerToPgTypeBuilder.build();

        TYPES = new HashSet<>(TYPE_TABLE.values());
        // the following polymorphic types are added manually,
        // because there are no corresponding data types in Cannerflow
        TYPES.add(AnyType.ANY);

        TYPES.forEach(type -> TYPE_NAME_TABLE.put(type.typName().toUpperCase(Locale.ROOT), type));
        TYPE_NAME_TABLE.put("REAL", REAL);
        TYPE_NAME_TABLE.put("DOUBLE", DOUBLE);
        TYPE_NAME_TABLE.put("BOOLEAN", BOOLEAN);
        TYPE_NAME_TABLE.put("INTEGER", INTEGER);
        TYPE_NAME_TABLE.put("SMALLINT", SMALLINT);
        TYPE_NAME_TABLE.put("BIGINT", BIGINT);
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

    public static PGType<?> getArrayType(int innerOid)
    {
        PGType<?> arrayType = INNER_TYPE_TO_ARRAY_TABLE.get(innerOid);
        if (arrayType == null) {
            throw new IllegalArgumentException(
                    format("No array type mapping from '%s' to pg_type", innerOid));
        }
        return arrayType;
    }

    public static Optional<PGType<?>> nameToPgType(String name)
    {
        return Optional.ofNullable(TYPE_NAME_TABLE.get(name.toUpperCase(Locale.ROOT)));
    }

    public static PGType<?> toPgRecordArray(PGType<?> innerRecordType)
    {
        return new PGArray(2287, innerRecordType);
    }
}
