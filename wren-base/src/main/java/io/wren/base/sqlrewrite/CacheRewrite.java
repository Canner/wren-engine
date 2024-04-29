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
package io.wren.base.sqlrewrite;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlBaseLexer;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import io.wren.base.CatalogSchemaTableName;
import io.wren.base.SessionContext;
import io.wren.base.WrenMDL;
import io.wren.base.sqlrewrite.analyzer.CacheAnalysis;
import io.wren.base.sqlrewrite.analyzer.Field;
import io.wren.base.sqlrewrite.analyzer.Scope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.sql.QueryUtil.getQualifiedName;
import static io.trino.sql.SqlFormatter.Dialect.DUCKDB;
import static io.wren.base.sqlrewrite.Utils.analyzeFrom;
import static io.wren.base.sqlrewrite.Utils.parseSql;
import static io.wren.base.sqlrewrite.Utils.toCatalogSchemaTableName;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class CacheRewrite
{
    private static final Logger LOG = Logger.get(CacheRewrite.class);
    private static final Set<String> KEYWORDS = ImmutableSet.copyOf(SqlBaseLexer.ruleNames);

    private CacheRewrite() {}

    public static Optional<String> rewrite(
            SessionContext sessionContext,
            String sql,
            Function<CatalogSchemaTableName, Optional<String>> converter,
            WrenMDL wrenMDL)
    {
        try {
            Statement statement = parseSql(sql);
            CacheAnalysis aggregationAnalysis = new CacheAnalysis();
            Statement rewritten = (Statement) new Rewriter(sessionContext, converter, wrenMDL, aggregationAnalysis).process(statement, Optional.empty());
            if (rewritten instanceof Query
                    && aggregationAnalysis.onlyCachedTables()) {
                return Optional.of(SqlFormatter.formatSql(rewritten, DUCKDB));
            }
        }
        catch (Exception e) {
            LOG.warn(e, "Failed to rewrite query: %s", sql);
        }
        return Optional.empty();
    }

    private static class Rewriter
            extends BaseRewriter<Optional<Scope>>
    {
        private final SessionContext sessionContext;
        private final Function<CatalogSchemaTableName, Optional<String>> converter;
        private final Map<QualifiedName, String> visitedAggregationTables = new HashMap<>();
        private final WrenMDL wrenMDL;
        private final CacheAnalysis aggregationAnalysis;

        public Rewriter(
                SessionContext sessionContext,
                Function<CatalogSchemaTableName, Optional<String>> converter,
                WrenMDL wrenMDL,
                CacheAnalysis aggregationAnalysis)
        {
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.converter = requireNonNull(converter, "converter is null");
            this.wrenMDL = requireNonNull(wrenMDL, "wrenMDL is null");
            this.aggregationAnalysis = requireNonNull(aggregationAnalysis, "aggregationAnalysis is null");
        }

        @Override
        protected Node visitQuery(Query node, Optional<Scope> scope)
        {
            Optional<Scope> withScope = analyzeWith(node, scope);
            return super.visitQuery(node, withScope);
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            Optional<Scope> relationScope;
            if (node.getFrom().isPresent()) {
                relationScope = Optional.of(analyzeFrom(wrenMDL, sessionContext, node.getFrom().get(), scope));
            }
            else {
                relationScope = scope;
            }
            return super.visitQuerySpecification(node, relationScope);
        }

        @Override
        protected Node visitJoin(Join node, Optional<Scope> scope)
        {
            return new Join(
                    node.getType(),
                    visitAndCast(node.getLeft(), scope),
                    visitAndCast(node.getRight(), scope),
                    node.getCriteria().map(criteria -> visitJoinCriteria(criteria, scope)));
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Optional<Scope> scope)
        {
            Expression base = node.getBase();
            if (scope.isPresent()) {
                List<Field> field = scope.get().getRelationType().resolveFields(getQualifiedName(node));
                if (field.size() == 1) {
                    QualifiedName qualifiedName = getQualifiedName(base);
                    if (field.get(0).getRelationAlias().isEmpty()
                            && visitedAggregationTables.containsKey(qualifiedName)) {
                        return new DereferenceExpression(
                                node.getLocation(),
                                DereferenceExpression.from(QualifiedName.of(visitedAggregationTables.get(qualifiedName))),
                                node.getField());
                    }
                }
            }
            return new DereferenceExpression(
                    node.getLocation(),
                    base,
                    node.getField());
        }

        @Override
        protected Node visitTable(Table node, Optional<Scope> scope)
        {
            if (scope.isPresent()) {
                Optional<WithQuery> withQuery = scope.get().getNamedQuery(node.getName().getSuffix());
                if (withQuery.isPresent()) {
                    return node;
                }
            }

            CatalogSchemaTableName catalogSchemaTableName = toCatalogSchemaTableName(sessionContext, node.getName());
            aggregationAnalysis.addTable(catalogSchemaTableName);
            if (wrenMDL.getCacheInfo(catalogSchemaTableName).isPresent()) {
                Optional<String> cachedTableOpt = convertTable(catalogSchemaTableName);
                if (cachedTableOpt.isPresent()) {
                    aggregationAnalysis.addCachedTables(catalogSchemaTableName);
                    String cachedTable = cachedTableOpt.get();
                    String schemaName = catalogSchemaTableName.getSchemaTableName().getSchemaName();
                    String tableName = catalogSchemaTableName.getSchemaTableName().getTableName();
                    visitedAggregationTables.put(QualifiedName.of(tableName), cachedTable);
                    visitedAggregationTables.put(QualifiedName.of(schemaName, tableName), cachedTable);
                    visitedAggregationTables.put(QualifiedName.of(catalogSchemaTableName.getCatalogName(), schemaName, tableName), cachedTable);
                    if (node.getLocation().isPresent()) {
                        return new Table(
                                node.getLocation().get(),
                                QualifiedName.of(cachedTable));
                    }
                    return new Table(QualifiedName.of(cachedTable));
                }
            }
            return node;
        }

        private Optional<String> convertTable(CatalogSchemaTableName cachedTable)
        {
            return converter.apply(cachedTable);
        }

        private Optional<Scope> analyzeWith(Query node, Optional<Scope> scope)
        {
            if (node.getWith().isEmpty()) {
                return Optional.of(Scope.builder().parent(scope).build());
            }

            With with = node.getWith().get();
            Scope.Builder withScopeBuilder = Scope.builder().parent(scope);

            for (WithQuery withQuery : with.getQueries()) {
                String name = withQuery.getName().getValue();
                if (withScopeBuilder.containsNamedQuery(name)) {
                    throw new IllegalArgumentException(format("WITH query name '%s' specified more than once", name));
                }
                if (with.isRecursive()) {
                    withScopeBuilder.namedQuery(name, withQuery);
                    visitAndCast(withQuery.getQuery(), Optional.of(withScopeBuilder.build()));
                }
                else {
                    visitAndCast(withQuery.getQuery(), Optional.of(withScopeBuilder.build()));
                    withScopeBuilder.namedQuery(name, withQuery);
                }
            }

            return Optional.of(withScopeBuilder.build());
        }
    }

    protected static Identifier identifier(String name)
    {
        if (KEYWORDS.contains(name.toUpperCase(ENGLISH))) {
            return new Identifier(name, true);
        }
        return new Identifier(name);
    }
}
