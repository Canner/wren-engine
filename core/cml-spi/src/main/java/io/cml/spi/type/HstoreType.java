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

import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A Hashmap type in PostgreSQL which keys and values are simple text value.
 * We used it to represent the presto Map type.
 * https://www.postgresql.org/docs/current/hstore.html
 * <p>
 * TODO: handle non-text keys and value.
 *  Because presto allows map with non-text key and value, we should handle this case.
 */
public class HstoreType
        extends PGType<Map<Object, Object>>
{
    public static final HstoreType HSTORE = new HstoreType();

    HstoreType()
    {
        super(57640, -1, -1, "hstore");
    }

    @Override
    public int typArray()
    {
        return PGArray.HSTORE_ARRAY.oid();
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

    /**
     * https://github.com/postgres/postgres/blob/master/contrib/hstore/hstore_io.c (hstore_send)
     */
    @Override
    public int writeAsBinary(ByteBuf buffer, Map<Object, Object> value)
    {
        int count = value.size();
        final int lenIndex = buffer.writerIndex();
        int bytesWritten = 4 + 4;
        buffer.writeInt(0);
        buffer.writeInt(count);

        int valueLen = 0;
        for (Map.Entry<Object, Object> entry : value.entrySet()) {
            byte[] key = entry.getKey().toString().getBytes(UTF_8);
            buffer.writeInt(key.length);
            buffer.writeBytes(key);
            valueLen += key.length + 4;
            if (entry.getValue() == null) {
                buffer.writeInt(-1);
                valueLen += 4;
            }
            else {
                byte[] val = entry.getValue().toString().getBytes(UTF_8);
                buffer.writeInt(val.length);
                buffer.writeBytes(val);
                valueLen += val.length + 4;
            }
        }
        int len = bytesWritten + valueLen;
        buffer.setInt(lenIndex, len);
        return INT32_BYTE_SIZE + len;
    }

    @Override
    public Map<Object, Object> readBinaryValue(ByteBuf buffer, int valueLength)
    {
        throw new UnsupportedOperationException("Input of anonymous hstore type values is not implemented");
    }

    /**
     * https://github.com/postgres/postgres/blob/master/contrib/hstore/hstore_io.c (hstore_out)
     */
    @Override
    public byte[] encodeAsUTF8Text(Map<Object, Object> value)
    {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<Object, Object> entry : value.entrySet()) {
            builder.append("\"").append(entry.getKey()).append("\"").append("=>");
            if (entry.getValue() == null) {
                builder.append("NULL");
            }
            else {
                builder.append("\"").append(entry.getValue()).append("\"");
            }
            builder.append(",");
        }
        builder.setLength(builder.length() - 1);
        return builder.toString().getBytes(UTF_8);
    }

    @Override
    public Map<Object, Object> decodeUTF8Text(byte[] bytes)
    {
        throw new UnsupportedOperationException("Input of anonymous hstore type values is not implemented");
    }
}
