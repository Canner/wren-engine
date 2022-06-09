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

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.Streams;
import io.cml.spi.ConnectorRecordIterable;
import io.cml.spi.type.PGType;
import io.cml.spi.type.VarcharType;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class BigQueryRecordIterable
        implements ConnectorRecordIterable
{
    private final List<PGType> types;
    private final TableResult tableResult;

    public static BigQueryRecordIterable of(TableResult tableResult)
    {
        return new BigQueryRecordIterable(tableResult);
    }

    private BigQueryRecordIterable(TableResult tableResult)
    {
        this.tableResult = requireNonNull(tableResult, "tableResult is null");

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
    public Iterator<Object[]> iterator()
    {
        Iterator<FieldValueList> iterator = tableResult.iterateAll().iterator();

        return new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Object[] next()
            {
                FieldValueList fieldValues = iterator.next();
                return fieldValues.stream()
                        // TODO: type mapping
                        .map(fieldValue -> fieldValue.getStringValue())
                        .toArray();
            }
        };
    }

    public List<PGType> getTypes()
    {
        return types;
    }
}
