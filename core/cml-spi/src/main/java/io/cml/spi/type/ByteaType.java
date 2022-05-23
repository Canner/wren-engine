/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.commons.codec.binary.Hex;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ByteaType
        extends PGType<Object>
{
    public static final int OID = 17;
    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    public static final ByteaType BYTEA = new ByteaType();

    private ByteaType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "bytea");
    }

    @Override
    public int typArray()
    {
        return PGArray.BYTEA_ARRAY.oid();
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
    public int writeAsBinary(ByteBuf buffer, Object value)
    {
        byte[] bytes = encodeHexString((byte[]) value).getBytes(UTF_8);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + bytes.length;
    }

    @Override
    public int writeAsText(ByteBuf buffer, @Nonnull Object value)
    {
        return writeAsBinary(buffer, value);
    }

    @Override
    public Object readBinaryValue(ByteBuf buffer, int valueLength)
    {
        checkArgument(valueLength >= 1, "The length of bytea should be 1 at least.");
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        return bytes;
    }

    @Override
    public byte[] encodeAsUTF8Text(Object value)
    {
        String strBuilder = "\\" + encodeHexString((byte[]) value);
        return strBuilder.getBytes(UTF_8);
    }

    @Override
    public Object decodeUTF8Text(byte[] bytes)
    {
        return bytes;
    }

    private String encodeHexString(byte[] decimalValue)
    {
        return "\\x" + Hex.encodeHexString(decimalValue);
    }
}
