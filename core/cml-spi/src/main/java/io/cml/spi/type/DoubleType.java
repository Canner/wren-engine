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

package io.cml.spi.type;

import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class DoubleType
        extends PGType<Double>
{
    public static final DoubleType DOUBLE = new DoubleType();
    static final int OID = 701;

    private static final int TYPE_LEN = 8;
    private static final int TYPE_MOD = -1;

    private DoubleType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "float8");
    }

    @Override
    public int typArray()
    {
        return PGArray.FLOAT8_ARRAY.oid();
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
    public int writeAsBinary(ByteBuf buffer, @Nonnull Double value)
    {
        buffer.writeInt(TYPE_LEN);
        buffer.writeDouble(value);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull Double value)
    {
        return Double.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Double readBinaryValue(ByteBuf buffer, int valueLength)
    {
        checkArgument(valueLength == TYPE_LEN, format("length should be %s because double is int64. Actual length: %s", TYPE_LEN, valueLength));
        return buffer.readDouble();
    }

    @Override
    public Double decodeUTF8Text(byte[] bytes)
    {
        return Double.parseDouble(new String(bytes, StandardCharsets.UTF_8));
    }
}
