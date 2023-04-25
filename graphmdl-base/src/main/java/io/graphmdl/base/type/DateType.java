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

import io.netty.buffer.ByteBuf;

import javax.annotation.Nonnull;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

public class DateType
        extends PGType
{
    public static final PGType DATE = new DateType();

    private static final int OID = 1082;
    private static final String NAME = "date";
    private static final int TYPE_LEN = 4;
    private static final int TYPE_MOD = -1;

    private static final DateTimeFormatter ISO_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .toFormatter(Locale.ENGLISH).withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter ISO_FORMATTER_AD = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd")
            .toFormatter(Locale.ENGLISH)
            .withResolverStyle(ResolverStyle.STRICT);

    private DateType()
    {
        super(OID, TYPE_LEN, TYPE_MOD, NAME);
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
    public int typArray()
    {
        return PGArray.DATE_ARRAY.oid();
    }

    @Override
    public byte[] encodeAsUTF8Text(@Nonnull Object value)
    {
        LocalDate date = LocalDate.parse((String) value);
        return date.format(ISO_FORMATTER_AD).getBytes(UTF_8);
    }

    @Override
    public Object decodeUTF8Text(byte[] bytes)
    {
        String s = new String(bytes, UTF_8);
        LocalDate dt = LocalDate.parse(s, ISO_FORMATTER);
        return Date.valueOf(dt);
    }

    @Override
    public int writeAsBinary(ByteBuf buffer, @Nonnull Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object readBinaryValue(ByteBuf buffer, int valueLength)
    {
        throw new UnsupportedOperationException();
    }
}
