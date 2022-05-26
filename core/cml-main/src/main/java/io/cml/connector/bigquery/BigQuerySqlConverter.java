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

import io.cml.calcite.CmlSchemaUtil;
import io.cml.calcite.SchemaPlusInfo;
import io.cml.spi.connector.Connector;
import io.cml.spi.metadata.TableMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class BigQuerySqlConverter
{
    private final SchemaPlusInfo schemaPlusInfo;

    @Inject
    public BigQuerySqlConverter(Connector bigQueryConnector)
    {
        final Connector connector = requireNonNull(bigQueryConnector, "BigQueryConnector is null");

        List<String> schemas = connector.listSchemas();
        Map<String, List<TableMetadata>> schemaTableMap = schemas.stream()
                .collect(toImmutableMap(identity(), schema -> connector.listTables(schema)));

        this.schemaPlusInfo = new SchemaPlusInfo(schemaTableMap);
    }

    public String convertSql(String sql)
    {
        return CmlSchemaUtil.convertQuery(CmlSchemaUtil.Dialect.BIGQUERY, schemaPlusInfo, sql);
    }
}
