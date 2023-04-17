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

package io.graphmdl.main.calcite;

import io.airlift.log.Logger;
import io.graphmdl.main.metadata.Metadata;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.util.Objects.requireNonNull;

public class QueryProcessor
{
    private static final Logger LOG = Logger.get(QueryProcessor.class);
    private final SqlDialect dialect;
    private final SqlParser sqlParser;
    private final Metadata metadata;

    public static QueryProcessor of(Metadata metadata)
    {
        return new QueryProcessor(metadata);
    }

    // TODO: abstract query processor https://github.com/Canner/canner-metric-layer/issues/68
    private QueryProcessor(Metadata metadata)
    {
        this.dialect = requireNonNull(metadata.getDialect().getSqlDialect(), "dialect is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = new SqlParser();
    }

    public String convert(String sql)
    {
        LOG.debug("[Input SQL]: %s", sql);
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter(SqlWriterConfig.of().withDialect(dialect));
        String result = sqlPrettyWriter.format(CalciteSqlNodeConverter.convert(statement, metadata));
        LOG.info("[Dialect SQL]: %s", result);
        return result;
    }
}
