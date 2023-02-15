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

import com.carrotsearch.hppc.ByteArrayList;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.graphmdl.base.type.PGArray.EMPTY_RECORD_ARRAY;

public class RecordType
        extends PGType<Map<String, Object>>
{
    public static final RecordType EMPTY_RECORD = new RecordType(List.of());
    private static final int OID = 2249;
    private static final String NAME = "record";

    private final List<PGType<?>> fieldTypes;

    RecordType(List<PGType<?>> fieldTypes)
    {
        super(OID, -1, -1, NAME);
        this.fieldTypes = fieldTypes;
    }

    @Override
    public int typArray()
    {
        return EMPTY_RECORD_ARRAY.oid();
    }

    @Override
    public String typeCategory()
    {
        return TypeCategory.PSEUDO.code();
    }

    @Override
    public String type()
    {
        return Type.PSEUDO.code();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public int writeAsBinary(ByteBuf buffer, Map<String, Object> record)
    {
        final int startWriterIndex = buffer.writerIndex();
        buffer.writeInt(0); // reserve space for the length of the record; updated later
        buffer.writeInt(fieldTypes.size());
        int bytesWritten = 4;
        List<Map.Entry<String, Object>> entries = record.entrySet().stream().collect(toImmutableList());
        for (int i = 0; i < fieldTypes.size(); i++) {
            PGType fieldType = fieldTypes.get(i);

            buffer.writeInt(fieldType.oid());
            bytesWritten += 4;

            Map.Entry<String, Object> entry = entries.get(i);
            if (entry.getValue() == null) {
                buffer.writeInt(-1); // -1 data length signals a NULL
                bytesWritten += 4;
                continue;
            }
            bytesWritten += fieldType.writeAsBinary(buffer, entry.getValue());
        }
        buffer.setInt(startWriterIndex, bytesWritten);
        return 4 + bytesWritten;
    }

    @Override
    public Map<String, Object> readBinaryValue(ByteBuf buffer, int valueLength)
    {
        throw new UnsupportedOperationException("Input of anonymous record type values is not implemented");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public byte[] encodeAsUTF8Text(Map<String, Object> record)
    {
        ByteArrayList bytes = new ByteArrayList();
        // See PostgreSQL src/backend/utils/adt/rowtypes.c record_out(PG_FUNCTION_ARGS)
        bytes.add((byte) '(');
        List<Map.Entry<String, Object>> rows = record.entrySet().stream().collect(toImmutableList());
        for (int i = 0; i < record.size(); i++) {
            PGType fieldType = fieldTypes.get(i);
            Map.Entry<String, Object> row = rows.get(i);

            if (i > 0) {
                bytes.add((byte) ',');
            }
            if (row.getValue() == null) {
                continue;
            }

            byte[] encodedValue = fieldType.encodeAsUTF8Text(row.getValue());
            boolean needQuotes = encodedValue.length == 0;
            for (byte b : encodedValue) {
                char c = (char) b;
                if (c == '"' || c == '\\' || c == '(' || c == ')' || c == ',' || Character.isWhitespace(c)) {
                    needQuotes = true;
                    break;
                }
            }
            if (needQuotes) {
                bytes.add((byte) '\"');
            }
            bytes.add(encodedValue);
            if (needQuotes) {
                bytes.add((byte) '\"');
            }
        }
        bytes.add((byte) ')');
        return bytes.toArray();
    }

    @Override
    public Map<String, Object> decodeUTF8Text(byte[] bytes)
    {
        throw new UnsupportedOperationException("Input of record type values is not implemented");
    }
}
