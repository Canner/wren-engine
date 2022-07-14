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

import io.cml.calcite.QueryProcessor;
import io.cml.metadata.Metadata;
import io.cml.spi.SessionContext;
import io.cml.sql.SqlConverter;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class BigQuerySqlConverter
        implements SqlConverter
{
    private final Metadata metadata;

    @Inject
    public BigQuerySqlConverter(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public String convert(String sql, SessionContext sessionContext)
    {
        QueryProcessor processor = QueryProcessor.of(metadata);
        return processor.convert(sql, sessionContext);
    }
}
