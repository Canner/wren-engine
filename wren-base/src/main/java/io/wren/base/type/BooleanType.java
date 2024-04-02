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

import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

import java.nio.ByteBuffer;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;

public class BooleanType
        extends PGType<Boolean>
{
    public static final PGType BOOLEAN = new BooleanType();
    static final int OID = 16;

    private static final int TYPE_LEN = 1;
    private static final int TYPE_MOD = -1;

    private static final byte[] TEXT_TRUE = new byte[] {'t'};
    private static final byte[] TEXT_FALSE = new byte[] {'f'};

    private static final Collection<ByteBuffer> TRUTH_VALUES = ImmutableSet.of(
            ByteBuffer.wrap(new byte[] {'1'}),
            ByteBuffer.wrap(new byte[] {'t'}),
            ByteBuffer.wrap(new byte[] {'T'}),
            ByteBuffer.wrap(new byte[] {'t', 'r', 'u', 'e'}),
            ByteBuffer.wrap(new byte[] {'T', 'R', 'U', 'E'}));

    private BooleanType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "bool");
    }

    @Override
    public int typArray()
    {
        return PGArray.BOOL_ARRAY.oid();
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
    public int writeAsBinary(ByteBuf buffer, @Nonnull Boolean value)
    {
        byte byteValue = (byte) (value ? 1 : 0);
        buffer.writeInt(TYPE_LEN);
        buffer.writeByte(byteValue);
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull Boolean value)
    {
        return value ? TEXT_TRUE : TEXT_FALSE;
    }

    @Override
    public Boolean readBinaryValue(ByteBuf buffer, int valueLength)
    {
        checkArgument(valueLength == TYPE_LEN, "length should be %s boolean is just a byte. Actual length: %s", TYPE_LEN, valueLength);
        byte value = buffer.readByte();
        switch (value) {
            case 0:
                return false;
            case 1:
                return true;
            default:
                throw new IllegalArgumentException("Unsupported binary bool: " + value);
        }
    }

    @Override
    public Boolean decodeUTF8Text(byte[] bytes)
    {
        return TRUTH_VALUES.contains(ByteBuffer.wrap(bytes));
    }

    @Override
    public Object getEmptyValue()
    {
        return false;
    }
}
