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

import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class PgOidVectorType
        extends PGType<List<Integer>>
{
    public static final PgOidVectorType INSTANCE = new PgOidVectorType();
    private static final int OID = 30;

    PgOidVectorType()
    {
        super(OID, -1, -1, "oidvector");
    }

    @Override
    public int typArray()
    {
        return 1013;
    }

    @Override
    public int typElem()
    {
        return OidType.OID;
    }

    @Override
    public String typeCategory()
    {
        return TypeCategory.ARRAY.code();
    }

    @Override
    public String type()
    {
        return Type.BASE.code();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int writeAsBinary(ByteBuf buffer, List<Integer> value)
    {
        return PGArray.INT4_ARRAY.writeAsBinary(buffer, (List) value);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Integer> readBinaryValue(ByteBuf buffer, int valueLength)
    {
        return (List<Integer>) (List) PGArray.INT4_ARRAY.readBinaryValue(buffer, valueLength);
    }

    @Override
    public byte[] encodeAsUTF8Text(List<Integer> value)
    {
        return Joiner.on(" ").join(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public List<Integer> decodeUTF8Text(byte[] bytes)
    {
        String string = new String(bytes, StandardCharsets.UTF_8);
        return listFromOidVectorString(string);
    }

    public static List<Integer> listFromOidVectorString(String value)
    {
        StringTokenizer tokenizer = new StringTokenizer(value, " ");
        ArrayList<Integer> oids = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            oids.add(Integer.parseInt(tokenizer.nextToken()));
        }
        return oids;
    }
}
