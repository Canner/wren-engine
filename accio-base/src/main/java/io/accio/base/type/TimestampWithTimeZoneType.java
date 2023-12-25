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

package io.accio.base.type;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.joda.time.format.DateTimeFormat;

import javax.annotation.Nonnull;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;

public class TimestampWithTimeZoneType
        extends BaseTimestampType
{
    public static final PGType TIMESTAMP_WITH_TIMEZONE = new TimestampWithTimeZoneType();
    public static final DateTimeFormatter PG_TIMESTAMP = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .optionalStart()
            .appendLiteral(' ')
            .optionalEnd()
            .appendPattern("[VV][x][xx][xxx][z]")
            .toFormatter(Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);

    public static final org.joda.time.format.DateTimeFormatter ISO_FORMATTER =
            DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSSZ").withLocale(Locale.ENGLISH);

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
        // TODO: consider AD, BC and dynamic fraction precision.
        if (value instanceof String) {
            return ((String) value).getBytes(UTF_8);
        }
        return ISO_FORMATTER.print(Instant.from((TemporalAccessor) value).getLong(MILLI_OF_SECOND))
                .getBytes(UTF_8);
    }

    @Override
    public Object decodeUTF8Text(byte[] bytes)
    {
        String dtString = new String(bytes, UTF_8);
        ZonedDateTime zonedDateTime = tryParse(dtString);
        return ISO_FORMATTER.print(Instant.from(zonedDateTime).toEpochMilli());
    }

    @VisibleForTesting
    ZonedDateTime tryParse(String timeString)
    {
        return ZonedDateTime.parse(timeString, PG_TIMESTAMP);
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Object value)
    {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
