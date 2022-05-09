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
import org.joda.time.Period;

import javax.annotation.Nonnull;

import java.nio.charset.StandardCharsets;

import static java.lang.Math.toIntExact;

public class IntervalType
        extends PGType<Period>
{
    private static final int OID = 1186;
    private static final int TYPE_LEN = 16;
    private static final int TYPE_MOD = -1;
    public static final IntervalType INTERVAL = new IntervalType();

    private IntervalType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, "interval");
    }

    @Override
    public int typArray()
    {
        // TODO support interval
        // return PGArray.INTERVAL_ARRAY.oid();
        throw new UnsupportedOperationException("no implementation");
    }

    @Override
    public String typeCategory()
    {
        return TypeCategory.TIMESPAN.code();
    }

    @Override
    public String type()
    {
        return Type.BASE.code();
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Period period)
    {
        buffer.writeInt(TYPE_LEN);
        // from PostgreSQL code:
        // pq_sendint64(&buf, interval->time);
        // pq_sendint32(&buf, interval->day);
        // pq_sendint32(&buf, interval->month);
        buffer.writeLong(
                (period.getHours() * 60 * 60 * 1000_000L)
                        + (period.getMinutes() * 60 * 1000_000L)
                        + (period.getSeconds() * 1000_000L)
                        + (period.getMillis() * 1000));
        buffer.writeInt((period.getWeeks() * 7) + period.getDays());
        buffer.writeInt((period.getYears() * 12) + period.getMonths());
        return INT32_BYTE_SIZE + TYPE_LEN;
    }

    @Override
    public Period readBinaryValue(ByteBuf buffer, int valueLength)
    {
        // assert valueLength == TYPE_LEN : "length should be " + TYPE_LEN + " because interval is 16. Actual length: " +
        //         valueLength;
        long micros = buffer.readLong();
        int days = buffer.readInt();
        int months = buffer.readInt();

        long microsInAnHour = 60 * 60 * 1000_000L;
        int hours = toIntExact(micros / microsInAnHour);
        long microsWithoutHours = micros % microsInAnHour;

        long microsInAMinute = 60 * 1000_000L;
        int minutes = toIntExact(microsWithoutHours / microsInAMinute);
        long microsWithoutMinutes = microsWithoutHours % microsInAMinute;

        int seconds = toIntExact(microsWithoutMinutes / 1000_000);
        int millis = toIntExact((microsWithoutMinutes % 1000_000) / 1000);
        return new Period(
                months / 12,
                months % 12,
                days / 7,
                days % 7,
                hours,
                minutes,
                seconds,
                millis);
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull Period value)
    {
        // StringBuilder sb = new StringBuilder();
        // IntervalType.PERIOD_FORMATTER.printTo(sb, (ReadablePeriod) value);
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Period decodeUTF8Text(byte[] bytes)
    {
        // return IntervalType.PERIOD_FORMATTER.parsePeriod(new String(bytes, StandardCharsets.UTF_8));
        return new Period(new String(bytes, StandardCharsets.UTF_8));
    }
}
