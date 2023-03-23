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
import com.google.common.collect.Streams;
import io.graphmdl.base.ConnectorRecordIterator;
import io.graphmdl.base.type.PGType;
import io.graphmdl.base.type.PGTypes;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
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

        this.types = Streams.stream(tableResult.getSchema().getFields().iterator())
                .map(field -> {
                    PGType<?> fieldType = BigQueryType.toPGType(field.getType().getStandardType());
                    if (field.getMode().equals(Field.Mode.REPEATED)) {
                        return PGTypes.getArrayType(fieldType.oid());
                    }
                    return fieldType;
                })
                .collect(toImmutableList());

        this.bqFields = tableResult.getSchema().getFields();
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean hasNext()
    {
        return resultIterator.hasNext();
    }

    @Override
    public Object[] next()
    {
        FieldValueList fieldValues = resultIterator.next();
        AtomicInteger index = new AtomicInteger(0);
        return fieldValues.stream()
                .map(fieldValue -> getFieldValue(index.getAndIncrement(), fieldValue))
                .toArray();
    }

    private Object getFieldValue(int index, FieldValue fieldValue)
    {
        if (fieldValue.isNull()) {
            return null;
        }

        StandardSQLTypeName typeName = bqFields.get(index).getType().getStandardType();
        if (bqFields.get(index).getMode().equals(Field.Mode.REPEATED)) {
            return fieldValue.getRepeatedValue().stream()
                    .map(innerField -> getFieldValue(typeName, innerField))
                    .collect(toImmutableList());
        }
        return getFieldValue(typeName, fieldValue);
    }

    private Object getFieldValue(StandardSQLTypeName typeName, FieldValue fieldValue)
    {
        switch (typeName) {
            case BOOL:
                return fieldValue.getBooleanValue();
            case INT64:
                return fieldValue.getLongValue();
            case FLOAT64:
                return fieldValue.getDoubleValue();
            case STRING:
                return fieldValue.getStringValue();
            case BYTES:
                return fieldValue.getBytesValue();
            case DATE:
                return fieldValue.getValue();
            default:
                throw new IllegalArgumentException("Unsupported type: " + typeName);
        }
    }

    public List<PGType> getTypes()
    {
        return types;
    }
}
