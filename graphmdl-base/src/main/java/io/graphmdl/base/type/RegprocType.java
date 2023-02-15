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

import javax.annotation.Nonnull;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RegprocType
        extends PGType<String>
{
    public static final RegprocType REGPROC = new RegprocType();

    private static final int OID = 24;
    // TODO: It's 4 in PostgreSQL because pg use oid to present this type but we use name.
    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    RegprocType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "regproc");
    }

    @Override
    public int typArray()
    {
        return PGArray.REGPROC_ARRAY.oid();
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
    public int writeAsBinary(ByteBuf buffer, @Nonnull String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + bytes.length;
    }

    @Override
    public int writeAsText(ByteBuf buffer, @Nonnull String value)
    {
        return writeAsBinary(buffer, value);
    }

    @Override
    public String readBinaryValue(ByteBuf buffer, int valueLength)
    {
        byte[] utf8 = new byte[valueLength];
        buffer.readBytes(utf8);
        return new String(utf8, UTF_8);
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
}
