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

import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

import java.nio.charset.StandardCharsets;

public abstract class PGType<T>
{
    public enum Type
    {
        BASE("b"),
        COMPOSITE("c"),
        ENUM("e"),
        DOMAIN("d"),
        PSEUDO("p"),
        RANGE("r");

        private final String code;

        Type(String code)
        {
            this.code = code;
        }

        public String code()
        {
            return code;
        }
    }

    public enum TypeCategory
    {
        ARRAY("A"),
        BOOLEAN("B"),
        COMPOSITE("C"),
        DATETIME("D"),
        GEOMETRIC("G"),
        NETWORK("I"),
        NUMERIC("N"),
        PSEUDO("P"),
        RANGE("R"),
        STRING("S"),
        TIMESPAN("T"),
        USER_DEFINED_TYPES("U"),
        BIT_STRING("V"),
        UNKNOWN("X");

        private final String code;

        TypeCategory(String code)
        {
            this.code = code;
        }

        public String code()
        {
            return code;
        }
    }

    public static final int INT32_BYTE_SIZE = Integer.SIZE / 8;
    private static final Logger LOGGER = Logger.get(PGType.class);

    private final int oid;
    private final int typeLen;
    private final int typeMod;
    private final String typName;

    protected PGType(int oid, int typeLen, int typeMod, @Nonnull String typName)
    {
        this.oid = oid;
        this.typeLen = typeLen;
        this.typeMod = typeMod;
        this.typName = typName;
    }

    public int oid()
    {
        return oid;
    }

    public short typeLen()
    {
        return (short) typeLen;
    }

    public abstract int typArray();

    public int typeMod()
    {
        return typeMod;
    }

    public String typName()
    {
        return typName;
    }

    public String typInput()
    {
        if (typArray() == 0) {
            return "array_in";
        }
        return "any_in";
    }

    public String typOutput()
    {
        if (typArray() == 0) {
            return "array_out";
        }
        return "any_out";
    }

    public String typReceive()
    {
        if (typArray() == 0) {
            return "array_recv";
        }
        return "any_recv";
    }

    public int typElem()
    {
        return 0;
    }

    public String typDelim()
    {
        return ",";
    }

    public abstract String typeCategory();

    public abstract String type();

    /**
     * Write the value as text into the buffer.
     * <p>
     * Format:
     * <pre>
     *  | int32 len (excluding len itself) | byte<b>N</b> value onto the buffer
     *  </pre>
     *
     * @return the number of bytes written. (4 (int32)  + N)
     */
    public int writeAsText(ByteBuf buffer, @Nonnull T value)
    {
        byte[] bytes = encodeAsUTF8Text(value);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
        return INT32_BYTE_SIZE + bytes.length;
    }

    public T readTextValue(ByteBuf buffer, int valueLength)
    {
        byte[] bytes = new byte[valueLength];
        buffer.readBytes(bytes);
        try {
            return decodeUTF8Text(bytes);
        }
        catch (Throwable t) {
            LOGGER.warn("decodeUTF8Text failed. input=%s type=%s",
                    new String(bytes, StandardCharsets.UTF_8), typName);
            throw t;
        }
    }

    /**
     * Write the value as binary into the buffer.
     * <p>
     * Format:
     * <pre>
     *  | int32 len (excluding len itself) | byte<b>N</b> value onto the buffer
     *  </pre>
     *
     * @return the number of bytes written. (4 (int32)  + N)
     */
    public abstract int writeAsBinary(ByteBuf buffer, @Nonnull T value);

    public abstract T readBinaryValue(ByteBuf buffer, int valueLength);

    /**
     * Return the UTF8 encoded text representation of the value
     */
    public abstract byte[] encodeAsUTF8Text(@Nonnull T value);

    /**
     * Convert a UTF8 encoded text representation into the actual value
     */
    public abstract T decodeUTF8Text(byte[] bytes);
}
