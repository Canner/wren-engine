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

package io.wren.base.type;

import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

import java.nio.charset.StandardCharsets;

import static io.wren.base.Utils.checkArgument;

public class SmallIntType
        extends PGType<Short>
{
    public static final SmallIntType SMALLINT = new SmallIntType();
    private static final int OID = 21;

    private static final int TYPE_LEN = 2;
    private static final int TYPE_MOD = -1;

    private SmallIntType()
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
    public int writeAsBinary(ByteBuf buffer, @Nonnull Short value)
    {
        buffer.writeInt(TYPE_LEN);
        buffer.writeShort(value);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull Short value)
    {
        return Short.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Short readBinaryValue(ByteBuf buffer, int valueLength)
    {
        checkArgument(valueLength == TYPE_LEN, "length should be %s because short is int16. Actual length: %s", TYPE_LEN, valueLength);
        return buffer.readShort();
    }

    @Override
    public Short decodeUTF8Text(byte[] bytes)
    {
        return Short.parseShort(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public Object getEmptyValue()
    {
        return 0;
    }
}
