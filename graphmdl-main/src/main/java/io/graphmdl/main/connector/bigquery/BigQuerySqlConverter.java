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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.graphmdl.base.SessionContext;
import io.graphmdl.main.metadata.Metadata;
import io.graphmdl.base.sql.SqlConverter;
import io.graphmdl.main.sql.SqlRewrite;
import io.graphmdl.main.sql.bigquery.FlattenGroupingElements;
import io.graphmdl.main.sql.bigquery.RemoveCatalogSchemaColumnPrefix;
import io.graphmdl.main.sql.bigquery.RemoveColumnAliasInAliasRelation;
import io.graphmdl.main.sql.bigquery.RemoveParameterInTypesInCast;
import io.graphmdl.main.sql.bigquery.ReplaceColumnAliasInUnnest;
import io.graphmdl.main.sql.bigquery.RewriteToBigQueryFunction;
import io.graphmdl.main.sql.bigquery.RewriteToBigQueryType;
import io.graphmdl.main.sql.bigquery.TransformCorrelatedJoinToJoin;
import io.trino.sql.tree.Node;
import org.intellij.lang.annotations.Language;

import javax.inject.Inject;

import java.util.List;

import static io.graphmdl.sqlrewrite.Utils.parseSql;
import static io.trino.sql.SqlFormatter.Dialect.BIGQUERY;
import static io.trino.sql.SqlFormatter.formatSql;
import static java.util.Objects.requireNonNull;

public class BigQuerySqlConverter
        implements SqlConverter
{
    private static final Logger LOG = Logger.get(BigQuerySqlConverter.class);
    private final Metadata metadata;

    @Inject
    public BigQuerySqlConverter(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public String convert(@Language("sql") String sql, SessionContext sessionContext)
    {
        Node rewrittenNode = parseSql(sql);

        List<SqlRewrite> sqlRewrites = ImmutableList.of(
                // bigquery doesn't support column name with catalog.schema.table prefix or schema.table prefix
                RemoveCatalogSchemaColumnPrefix.INSTANCE,
                // bigquery doesn't support column alias in alias relation
                RemoveColumnAliasInAliasRelation.INSTANCE,
                // bigquery doesn't support column alias in unnest alias relation
                ReplaceColumnAliasInUnnest.INSTANCE,
                // bigquery doesn't support correlated join in where clause
                TransformCorrelatedJoinToJoin.INSTANCE,
                RewriteToBigQueryFunction.INSTANCE,
                RewriteToBigQueryType.INSTANCE,
                // bigquery doesn't support parameter in types in cast
                // this should happen after RewriteToBigQueryType since RewriteToBigQueryType will replace
                // GenericLiteral with Cast and types in Cast could contain parameter.
                RemoveParameterInTypesInCast.INSTANCE,
                FlattenGroupingElements.INSTANCE);

        LOG.info("[Input sql]: %s", sql);

        for (SqlRewrite rewrite : sqlRewrites) {
            LOG.debug("Before %s: %s", rewrite.getClass().getSimpleName(), formatSql(rewrittenNode));
            rewrittenNode = rewrite.rewrite(rewrittenNode, metadata);
            LOG.debug("After %s: %s", rewrite.getClass().getSimpleName(), formatSql(rewrittenNode));
        }

        String dialectSql = formatSql(rewrittenNode, BIGQUERY);
        LOG.info("[Dialect sql]: %s", dialectSql);
        return dialectSql;
    }
}
