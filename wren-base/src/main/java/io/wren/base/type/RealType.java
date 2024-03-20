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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class RealType
        extends PGType<Float>
{
    public static final RealType REAL = new RealType();
    static final int OID = 700;

    private static final int TYPE_LEN = 4;
    private static final int TYPE_MOD = -1;

    private RealType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "float4");
    }

    @Override
    public int typArray()
    {
        return PGArray.FLOAT4_ARRAY.oid();
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
    public int writeAsBinary(ByteBuf buffer, @Nonnull Float value)
    {
        buffer.writeInt(TYPE_LEN);
        buffer.writeFloat(value);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull Float value)
    {
        return Float.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Float readBinaryValue(ByteBuf buffer, int valueLength)
    {
        checkArgument(valueLength == TYPE_LEN, format("length should be %s because float is int32. Actual length: %s", TYPE_LEN, valueLength));
        return buffer.readFloat();
    }

    @Override
    public Float decodeUTF8Text(byte[] bytes)
    {
        return Float.parseFloat(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public Object getEmptyValue()
    {
        return 0.0f;
    }
}
