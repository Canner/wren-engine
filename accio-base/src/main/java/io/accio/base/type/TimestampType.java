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

import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.util.Locale.ENGLISH;

public class TimestampType
        extends BaseTimestampType
{
    public static final PGType TIMESTAMP = new TimestampType();

    private static final int OID = 1114;
    private static final String NAME = "timestamp";

    // TODO support timestamp with precision dynamically
    // BigQuery support precision with 6
    private static final DateTimeFormatter PG_TIMESTAMP = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
            .toFormatter(ENGLISH)
            .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter PARSER_WITH_OPTIONAL_ERA = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .optionalStart()
            .appendPattern("[VV][x][xx][xxx]")
            .optionalStart()
            .appendLiteral(' ')
            .appendPattern("G")
            .toFormatter(Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);

    private TimestampType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, NAME);
    }

    @Override
    public int typArray()
    {
        return PGArray.TIMESTAMP_ARRAY.oid();
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull Object value)
    {
        LocalDateTime dt = (LocalDateTime) value;
        return PG_TIMESTAMP.format(dt).getBytes(UTF_8);
    }

    @Override
    public Object decodeUTF8Text(byte[] bytes)
    {
        String s = new String(bytes, UTF_8);
        LocalDateTime dt = LocalDateTime.parse(s, PARSER_WITH_OPTIONAL_ERA);
        return PG_TIMESTAMP.format(dt);
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getEmptyValue()
    {
        return "1970-01-01 00:00:00";
    }
}
