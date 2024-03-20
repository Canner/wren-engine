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

package io.wren.base.type;

import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

abstract class BaseTimestampType
        extends PGType
{
    protected static final int TYPE_LEN = 8;
    protected static final int TYPE_MOD = -1;

    BaseTimestampType(int oid, int typeLen, int typeMod, @Nonnull String typeName)
    {
        super(oid, typeLen, typeMod, typeName);
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Object value)
    {
        buffer.writeInt(TYPE_LEN);
        buffer.writeLong(PgDatetimeUtils.toPgTimestamp((long) value));
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public String typeCategory()
    {
        return TypeCategory.DATETIME.code();
    }

    @Override
    public String type()
    {
        return Type.BASE.code();
    }

    @Override
    public Object readBinaryValue(ByteBuf buffer, int valueLength)
    {
        checkArgument(valueLength == TYPE_LEN, format("valueLength must be %s because timestamp is a 64 bit long. Actual length is %s", TYPE_LEN, valueLength));
        long microSecondsSince2K = buffer.readLong();
        return PgDatetimeUtils.toTrinoTimestamp(microSecondsSince2K);
    }
}
