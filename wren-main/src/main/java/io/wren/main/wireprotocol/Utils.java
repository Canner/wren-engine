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

package io.wren.main.wireprotocol;

import io.netty.buffer.ByteBuf;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Utils
{
    private Utils() {}

    @Nullable
    public static String readCString(ByteBuf buffer)
    {
        byte[] bytes = new byte[buffer.bytesBefore((byte) 0) + 1];
        if (bytes.length == 0) {
            return null;
        }
        buffer.readBytes(bytes);
        return new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8);
    }

    @Nullable
    public static char[] readCharArray(ByteBuf buffer)
    {
        byte[] bytes = new byte[buffer.bytesBefore((byte) 0)];
        if (bytes.length == 0) {
            return null;
        }
        buffer.readBytes(bytes);
        return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(bytes)).array();
    }
}
