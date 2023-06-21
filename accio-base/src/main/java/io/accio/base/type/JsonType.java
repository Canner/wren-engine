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

import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JsonType
        extends PGType<String>
{
    public static final JsonType JSON = new JsonType();
    static final int OID = 114;

    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    private JsonType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "json");
    }

    @Override
    public int typArray()
    {
        return PGArray.JSON_ARRAY.oid();
    }

    @Override
    public String typeCategory()
    {
        return TypeCategory.USER_DEFINED_TYPES.code();
    }

    @Override
    public String type()
    {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull String value)
    {
        byte[] bytes = encodeAsUTF8Text(value);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + bytes.length;
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull String value)
    {
        return value.getBytes(UTF_8);
    }

    @Override
    public String readBinaryValue(ByteBuf buffer, int valueLength)
    {
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        return decodeUTF8Text(bytes);
    }

    @Override
    public String decodeUTF8Text(byte[] bytes)
    {
        return new String(bytes, UTF_8);
    }
}
