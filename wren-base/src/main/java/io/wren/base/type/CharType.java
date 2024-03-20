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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CharType
        extends PGType<String>
{
    public static final CharType CHAR = new CharType();
    static final int OID = 18;

    private CharType()
    {
        super(OID, 1, -1, "char");
    }

    @Override
    public int typArray()
    {
        return PGArray.CHAR_ARRAY.oid();
    }

    @Override
    public String typeCategory()
    {
        return TypeCategory.STRING.code();
    }

    @Override
    public String type()
    {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull String value)
    {
        buffer.writeInt(1);
        buffer.writeBytes(value.getBytes(UTF_8));
        return 5;
    }

    @Override
    public String readBinaryValue(ByteBuf buffer, int valueLength)
    {
        checkArgument(valueLength == 1, "The length of char should be 1");
        return new String(buffer.readBytes(valueLength).array(), UTF_8);
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull String value)
    {
        return value.getBytes(UTF_8);
    }

    @Override
    public String decodeUTF8Text(byte[] bytes)
    {
        return new String(bytes, UTF_8);
    }

    @Override
    public Object getEmptyValue()
    {
        return "";
    }
}
