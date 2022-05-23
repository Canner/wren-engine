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

import javax.annotation.Nonnull;

import static java.nio.charset.StandardCharsets.UTF_8;

public class InetType
        extends PGType<String>
{
    public static final InetType INET = new InetType();
    static final int OID = 869;

    private static final int TYPE_LEN = -1;
    private static final int TYPE_MOD = -1;

    private InetType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "inet");
    }

    @Override
    public int typArray()
    {
        return PGArray.INET_ARRAY.oid();
    }

    @Override
    public String typeCategory()
    {
        return TypeCategory.NETWORK.code();
    }

    @Override
    public String type()
    {
        return PGType.Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull String value)
    {
        throw new UnsupportedOperationException("InetType doesn't support binary format.");
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull String value)
    {
        return value.getBytes(UTF_8);
    }

    @Override
    public String readBinaryValue(ByteBuf buffer, int valueLength)
    {
        throw new UnsupportedOperationException("InetType doesn't support binary format.");
    }

    @Override
    public String decodeUTF8Text(byte[] bytes)
    {
        return new String(bytes, UTF_8);
    }
}
