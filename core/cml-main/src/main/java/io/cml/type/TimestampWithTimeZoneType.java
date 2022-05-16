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

package io.cml.type;

import io.cml.spi.type.PGType;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

public class TimestampWithTimeZoneType
        extends BaseTimestampType
{
    public static final PGType TIMESTAMP_WITH_TIMEZONE = new TimestampWithTimeZoneType();

    private static final int OID = 1184;
    private static final String NAME = "timestamptz";

    private TimestampWithTimeZoneType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, NAME);
    }

    @Override
    public int typArray()
    {
        return PGArray.TIMESTAMP_WITH_TIMEZONE_ARRAY.oid();
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object decodeUTF8Text(byte[] bytes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Object value)
    {
        throw new UnsupportedOperationException();
    }
}
