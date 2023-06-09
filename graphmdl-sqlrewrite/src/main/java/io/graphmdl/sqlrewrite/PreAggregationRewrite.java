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
package io.graphmdl.sqlrewrite;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.GraphMDL;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.dto.Metric;
import io.graphmdl.sqlrewrite.analyzer.Field;
import io.graphmdl.sqlrewrite.analyzer.PreAggregationAnalysis;
import io.graphmdl.sqlrewrite.analyzer.Scope;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlBaseLexer;
import io.trino.sql.parser.SqlParser;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.graphmdl.sqlrewrite.Utils.analyzeFrom;
import static io.graphmdl.sqlrewrite.Utils.toCatalogSchemaTableName;
import static io.trino.sql.QueryUtil.getQualifiedName;
import static io.trino.sql.SqlFormatter.Dialect.DUCKDB;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PreAggregationRewrite
{
    private static final Logger LOG = Logger.get(PreAggregationRewrite.class);
    private static final Set<String> KEYWORDS = ImmutableSet.copyOf(SqlBaseLexer.ruleNames);
    private static final SqlParser SQL_PARSER = new SqlParser();

    private PreAggregationRewrite() {}

    public static Optional<String> rewrite(
            SessionContext sessionContext,
            String sql,
            Function<CatalogSchemaTableName, Optional<String>> converter,
            GraphMDL graphMDL)
    {
        try {
            Statement statement = SQL_PARSER.createStatement(sql, new ParsingOptions(AS_DECIMAL));
            PreAggregationAnalysis aggregationAnalysis = new PreAggregationAnalysis();
            Statement rewritten = (Statement) new Rewriter(sessionContext, converter, graphMDL, aggregationAnalysis).process(statement, Optional.empty());
            if (rewritten instanceof Query
                    && aggregationAnalysis.onlyPreAggregationTables()) {
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
        private final GraphMDL graphMDL;
        private final PreAggregationAnalysis aggregationAnalysis;

        public Rewriter(
                SessionContext sessionContext,
                Function<CatalogSchemaTableName, Optional<String>> converter,
                GraphMDL graphMDL,
                PreAggregationAnalysis aggregationAnalysis)
        {
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.converter = requireNonNull(converter, "converter is null");
            this.graphMDL = requireNonNull(graphMDL, "graphMDL is null");
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
                relationScope = Optional.of(analyzeFrom(graphMDL, sessionContext, node.getFrom().get(), scope));
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
                List<Field> field = scope.get().getRelationType().get().resolveFields(getQualifiedName(node));
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
            if (graphMDL.getMetric(catalogSchemaTableName).filter(Metric::isPreAggregated).isPresent()) {
                Optional<String> preAggregationTableOpt = convertTable(catalogSchemaTableName);
                if (preAggregationTableOpt.isPresent()) {
                    aggregationAnalysis.addPreAggregationTables(catalogSchemaTableName);
                    String preAggregationTable = preAggregationTableOpt.get();
                    String schemaName = catalogSchemaTableName.getSchemaTableName().getSchemaName();
                    String tableName = catalogSchemaTableName.getSchemaTableName().getTableName();
                    visitedAggregationTables.put(QualifiedName.of(tableName), preAggregationTable);
                    visitedAggregationTables.put(QualifiedName.of(schemaName, tableName), preAggregationTable);
                    visitedAggregationTables.put(QualifiedName.of(catalogSchemaTableName.getCatalogName(), schemaName, tableName), preAggregationTable);
                    if (node.getLocation().isPresent()) {
                        return new Table(
                                node.getLocation().get(),
                                QualifiedName.of(preAggregationTable));
                    }
                    return new Table(QualifiedName.of(preAggregationTable));
                }
            }
            return node;
        }

        private Optional<String> convertTable(CatalogSchemaTableName preAggregationTable)
        {
            return converter.apply(preAggregationTable);
        }

        // TODO: from StatementAnalyzer.analyzeWith will recursive query mess up anything here?
        private Optional<Scope> analyzeWith(Query node, Optional<Scope> scope)
        {
            if (node.getWith().isEmpty()) {
                return Optional.empty();
            }

            With with = node.getWith().get();
            Scope.Builder withScopeBuilder = Scope.builder().parent(scope);

            for (WithQuery withQuery : with.getQueries()) {
                String name = withQuery.getName().getValue();
                if (withScopeBuilder.containsNamedQuery(name)) {
                    throw new IllegalArgumentException(format("WITH query name '%s' specified more than once", name));
                }
                visitAndCast(withQuery.getQuery(), Optional.of(withScopeBuilder.build()));
                withScopeBuilder.namedQuery(name, withQuery);
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
