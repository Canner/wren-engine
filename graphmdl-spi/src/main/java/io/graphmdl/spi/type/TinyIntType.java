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

package io.graphmdl.spi.type;

import io.graphmdl.spi.CmlException;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

import static io.graphmdl.spi.metadata.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class is write-only class. We never read any data by it and won't show it to pg_type list.
 * It is a wrap of SmallIntType. Actually, PostgreSQL doesn't have TinyInt.
 * To handle Presto TinyInt, we use this to transform Presto TinyInt to Pg SmallInt.
 */
public class TinyIntType
        extends PGType<Byte>
{
    public static final TinyIntType TINYINT = new TinyIntType();
    private static final int OID = 21;

    private static final int TYPE_LEN = 2;
    private static final int TYPE_MOD = -1;

    private TinyIntType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "int2");
    }

    @Override
    public int typArray()
    {
        return PGArray.INT2_ARRAY.oid();
    }

    @Override
    public String typeCategory()
    {
        return TypeCategory.NUMERIC.code();
    }

    @Override
    public String type()
    {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Byte value)
    {
        buffer.writeInt(TYPE_LEN);
        buffer.writeShort(value);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull Byte value)
    {
        return Byte.toString(value).getBytes(UTF_8);
    }

    @Override
    public Byte readBinaryValue(ByteBuf buffer, int valueLength)
    {
        throw new CmlException(GENERIC_INTERNAL_ERROR, new IllegalAccessException("PostgreSQL doesn't have TinyIntType. We never read TinyInt from client."));
    }

    @Override
    public Byte decodeUTF8Text(byte[] bytes)
    {
        throw new CmlException(GENERIC_INTERNAL_ERROR, new IllegalAccessException("PostgreSQL doesn't have TinyIntType. We never read TinyInt from client."));
    }
}
