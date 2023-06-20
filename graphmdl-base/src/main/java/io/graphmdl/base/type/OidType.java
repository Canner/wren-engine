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

package io.graphmdl.base.type;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class OidType
        extends PGType<Long>
{
    public static final OidType OID_INSTANCE = new OidType();

    static final int OID = 26;

    private static final int TYPE_LEN = 8;
    private static final int TYPE_MOD = -1;

    OidType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "oid");
    }

    @Override
    public int typArray()
    {
        return PGArray.OID_ARRAY.oid();
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
    public int writeAsBinary(ByteBuf buffer, Long value)
    {
        buffer.writeInt(TYPE_LEN);
        buffer.writeLong(value);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public Long readBinaryValue(ByteBuf buffer, int valueLength)
    {
        checkArgument(valueLength == TYPE_LEN, format("length should be %s because oid is int32. Actual length: %s", TYPE_LEN, valueLength));
        return buffer.readLong();
    }

    @Override
    public byte[] encodeAsUTF8Text(Long value)
    {
        return Long.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Long decodeUTF8Text(byte[] bytes)
    {
        return Long.parseLong(new String(bytes, StandardCharsets.UTF_8));
    }
}
