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

package io.cml.connector.bigquery;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.Streams;
import io.cml.spi.ConnectorRecordIterator;
import io.cml.spi.type.PGType;
import io.cml.spi.type.VarcharType;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class BigQueryRecordIterator
        implements ConnectorRecordIterator
{
    private final List<PGType> types;
    private final Iterator<FieldValueList> resultIterator;

    public static BigQueryRecordIterator of(TableResult tableResult)
    {
        return new BigQueryRecordIterator(tableResult);
    }

    private BigQueryRecordIterator(TableResult tableResult)
    {
        requireNonNull(tableResult, "tableResult is null");
        this.resultIterator = tableResult.iterateAll().iterator();

        // TODO: type mapping
        this.types = Streams.stream(tableResult.getSchema().getFields().iterator())
                .map(field -> VarcharType.VARCHAR)
                .collect(toImmutableList());
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
        return fieldValues.stream()
                // TODO: type mapping
                .map(FieldValue::getStringValue)
                .toArray();
    }

    public List<PGType> getTypes()
    {
        return types;
    }
}
