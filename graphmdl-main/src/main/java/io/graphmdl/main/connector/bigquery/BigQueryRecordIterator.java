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

package io.graphmdl.main.connector.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.type.PGType;
import io.graphmdl.connector.bigquery.BigQueryType;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Period;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.cloud.bigquery.StandardSQLTypeName.STRUCT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class BigQueryRecordIterator
        implements ConnectorRecordIterator
{
    private final List<PGType> types;
    private final List<Field> bqFields;

    private final Iterator<FieldValueList> resultIterator;

    public static BigQueryRecordIterator of(TableResult tableResult)
    {
        return new BigQueryRecordIterator(tableResult);
    }

    private BigQueryRecordIterator(TableResult tableResult)
    {
        requireNonNull(tableResult, "tableResult is null");
        this.resultIterator = tableResult.iterateAll().iterator();

        this.types = tableResult.getSchema().getFields().stream()
                .map(BigQueryType::toPGType)
                .collect(toImmutableList());

        this.bqFields = tableResult.getSchema().getFields();
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext()
    {
        return resultIterator.hasNext();
    }

    @Override
    public Object[] next()
    {
        FieldValueList fieldValues = resultIterator.next();
        return bqFields.stream()
                .map(field -> getFieldValue(field, fieldValues.get(field.getName())))
                .toArray();
    }

    private static Object getFieldValue(Field field, FieldValue fieldValue)
    {
        if (fieldValue.isNull()) {
            return null;
        }

        StandardSQLTypeName typeName = field.getType().getStandardType();
        if (field.getMode().equals(Field.Mode.REPEATED)) {
            return fieldValue.getRepeatedValue().stream()
                    .map(innerField -> getFieldValue(typeName, innerField))
                    .collect(toImmutableList());
        }

        if (typeName.equals(STRUCT)) {
            List<Field> subFields = field.getSubFields();
            List<FieldValue> subFieldValues = fieldValue.getRecordValue();
            Map<String, Object> result = new HashMap<>();
            for (int i = 0; i < subFields.size(); i++) {
                result.put(subFields.get(i).getName(), getFieldValue(subFields.get(i), subFieldValues.get(i)));
            }
            return result;
        }
        return getFieldValue(typeName, fieldValue);
    }

    private static Object getFieldValue(StandardSQLTypeName typeName, FieldValue fieldValue)
    {
        switch (typeName) {
            case BOOL:
                return fieldValue.getBooleanValue();
            case INT64:
                return fieldValue.getLongValue();
            case FLOAT64:
                return fieldValue.getDoubleValue();
            case STRING:
            case JSON:
                return fieldValue.getStringValue();
            case BYTES:
                return fieldValue.getBytesValue();
            case DATE:
                return fieldValue.getValue();
            case DATETIME:
                return convertToMicroseconds(LocalDateTime.parse(fieldValue.getStringValue()));
            case TIMESTAMP:
                return fieldValue.getTimestampValue();
            case NUMERIC:
            case BIGNUMERIC:
                return fieldValue.getNumericValue();
            case INTERVAL:
                return convertBigQueryIntervalToPeriod(fieldValue.getStringValue());
            default:
                throw new IllegalArgumentException("Unsupported type: " + typeName);
        }
    }

    public List<PGType> getTypes()
    {
        return types;
    }

    private static Period convertBigQueryIntervalToPeriod(String value)
    {
        // BigQuery interval format: [sign]Y-M [sign]D [sign]H:M:S[.F], and F up to six digits
        Pattern pattern = Pattern.compile("(?<NEG>-?)(?<Y>[0-9]+)-(?<M>[0-9]+) (?<D>-?[0-9]+) (?<NEGTIME>-?)(?<H>[0-9]+):(?<MIN>[0-9]+):(?<S>[0-9]+).?(?<F>[0-9]{1,6})?");
        Matcher matcher = pattern.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(format(ENGLISH, "Invalid interval format: %s", value));
        }
        int sign = matcher.group("NEG").isEmpty() ? 1 : -1;
        int year = Integer.parseInt(matcher.group("Y")) * sign;
        int mon = Integer.parseInt(matcher.group("M")) * sign;
        int day = Integer.parseInt(matcher.group("D"));
        int signTime = matcher.group("NEGTIME").isEmpty() ? 1 : -1;
        int hour = Integer.parseInt(matcher.group("H")) * signTime;
        int min = Integer.parseInt(matcher.group("MIN")) * signTime;
        int sec = Integer.parseInt(matcher.group("S")) * signTime;
        if (matcher.group("F") == null) {
            return new Period(year, mon, 0, day, hour, min, sec, 0);
        }
        String micro = StringUtils.rightPad(matcher.group("F"), 6, '0');
        return new Period(year, mon, 0, day, hour, min, sec, Integer.parseInt(micro) / 1000);
    }

    private static long convertToMicroseconds(LocalDateTime localDateTime)
    {
        return (localDateTime.toInstant(UTC).getEpochSecond() * 1000000) + (localDateTime.getNano() / 1000);
    }
}
