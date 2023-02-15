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

import java.nio.charset.StandardCharsets;

public class VarcharType
        extends PGType<String>
{
    static final int OID = 1043;
    private static final int ARRAY_OID = 1015;
    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    public static final VarcharType VARCHAR = new VarcharType(ARRAY_OID);

    private final int typArray;

    private VarcharType(int typArray)
    {
        super(OID, TYPE_LEN, TYPE_MOD, "varchar");
        this.typArray = typArray;
    }

    private VarcharType(int oid, int typArray, int maxLength, String aliasName)
    {
        super(oid, maxLength, TYPE_MOD, aliasName);
        this.typArray = typArray;
    }

    @Override
    public int typArray()
    {
        return typArray;
    }

    @Override
    public String type()
    {
        return Type.BASE.code();
    }

    @Override
    public String typeCategory()
    {
        return TypeCategory.STRING.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull String value)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
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
    public byte[] encodeAsUTF8Text(@Nonnull String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String readBinaryValue(ByteBuf buffer, int valueLength)
    {
        byte[] utf8 = new byte[valueLength];
        buffer.readBytes(utf8);
        return new String(utf8, StandardCharsets.UTF_8);
    }

    @Override
    public String decodeUTF8Text(byte[] bytes)
    {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static class NameType
    {
        static final int OID = 19;
        private static final int ARRAY_OID = -1;
        private static final int TYPE_LEN = 64;

        public static final VarcharType NAME = new VarcharType(OID, ARRAY_OID, TYPE_LEN, "name");
    }

    public static class TextType
    {
        static final int OID = 25;
        static final int TEXT_ARRAY_OID = 1009;
        public static final VarcharType TEXT = new VarcharType(OID, TEXT_ARRAY_OID, -1, "text");
    }
}
