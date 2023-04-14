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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.GraphMDLException;
import io.graphmdl.base.SessionContext;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlBaseLexer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.graphmdl.base.metadata.StandardErrorCode.GENERIC_USER_ERROR;
import static io.graphmdl.sqlrewrite.Utils.toCatalogSchemaTableName;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class PreAggregationRewrite
{
    private static final String UNNAMED_ALIAS_PREFIX = "_alias_";
    private static final Set<String> KEYWORDS = ImmutableSet.copyOf(SqlBaseLexer.ruleNames);

    private PreAggregationRewrite() {}

    public static Statement rewrite(
            SessionContext sessionContext,
            SqlParser sqlParser,
            String sql,
            Function<CatalogSchemaTableName, Optional<String>> converter)
    {
        return (Statement) new Rewriter(sessionContext, converter).process(sqlParser.createStatement(sql, new ParsingOptions(AS_DECIMAL)));
    }

    private static class Rewriter
            extends BaseRewriter<Void>
    {
        private final SessionContext sessionContext;
        private final Function<CatalogSchemaTableName, Optional<String>> converter;
        private final Set<Identifier> visitedAlias = new HashSet<>();
        private final Map<Expression, Identifier> joinTableAliasMapping = new HashMap<>();
        private final Map<QualifiedName, String> visitedAggregationTables = new HashMap<>();

        public Rewriter(
                SessionContext sessionContext,
                Function<CatalogSchemaTableName, Optional<String>> converter)
        {
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.converter = requireNonNull(converter, "converter is null");
        }

        @Override
        protected Node visitJoin(Join node, Void context)
        {
            List<Relation> joinRelations = ImmutableList.of(node.getLeft(), node.getRight()).stream()
                    .map(relation -> {
                        if (relation instanceof Table) {
                            Table table = (Table) relation;
                            Optional<CatalogSchemaTableName> preAggregationTableKey = getPreAggregationKey(table.getName());
                            if (preAggregationTableKey.isPresent()) {
                                CatalogSchemaTableName catalogSchemaTableName = preAggregationTableKey.get();
                                String tableName = catalogSchemaTableName.getSchemaTableName().getTableName();
                                String schemaName = catalogSchemaTableName.getSchemaTableName().getSchemaName();
                                Identifier aliasName = new Identifier(UNNAMED_ALIAS_PREFIX + tableName);
                                joinTableAliasMapping.put(
                                        DereferenceExpression.from(QualifiedName.of(tableName)),
                                        aliasName);
                                joinTableAliasMapping.put(
                                        DereferenceExpression.from(QualifiedName.of(schemaName, tableName)),
                                        aliasName);
                                joinTableAliasMapping.put(
                                        DereferenceExpression.from(QualifiedName.of(catalogSchemaTableName.getCatalogName(), schemaName, tableName)),
                                        aliasName);
                                if (table.getLocation().isPresent()) {
                                    new AliasedRelation(table.getLocation().get(), table, aliasName, null);
                                }
                                return new AliasedRelation(table, aliasName, null);
                            }
                        }
                        return relation;
                    })
                    .collect(toImmutableList());

            if (node.getLocation().isPresent()) {
                return new Join(
                        node.getLocation().get(),
                        node.getType(),
                        visitAndCast(joinRelations.get(0), context),
                        visitAndCast(joinRelations.get(1), context),
                        node.getCriteria().map(criteria -> visitJoinCriteria(criteria, context)));
            }
            return new Join(
                    node.getType(),
                    visitAndCast(joinRelations.get(0), context),
                    visitAndCast(joinRelations.get(1), context),
                    node.getCriteria().map(criteria -> visitJoinCriteria(criteria, context)));
        }

        @Override
        protected Node visitAliasedRelation(AliasedRelation node, Void context)
        {
            visitedAlias.add(node.getAlias());
            return super.visitAliasedRelation(node, context);
        }

        @Override
        protected Node visitWithQuery(WithQuery node, Void context)
        {
            visitedAlias.add(new Identifier(node.getName().getValue().toLowerCase(ENGLISH)));
            return super.visitWithQuery(node, context);
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            QualifiedName qualifiedName = getQualifiedName(node.getBase());
            Expression base;
            if (qualifiedName != null && !visitedAlias.contains(node.getBase()) && visitedAggregationTables.containsKey(qualifiedName)) {
                String cachedTable = visitedAggregationTables.get(qualifiedName);
                QualifiedName dereferenceName = joinTableAliasMapping.containsKey(node.getBase()) ?
                        QualifiedName.of(joinTableAliasMapping.get(node.getBase()).getValue()) :
                        QualifiedName.of(cachedTable);
                base = DereferenceExpression.from(dereferenceName);
            }
            else {
                base = visitAndCast(node.getBase(), context);
            }

            return new DereferenceExpression(
                    node.getLocation(),
                    base,
                    node.getField());
        }

        @Override
        protected Node visitTable(Table node, Void context)
        {
            Optional<CatalogSchemaTableName> preAggregationTableKeyOpt = getPreAggregationKey(node.getName());

            if (preAggregationTableKeyOpt.isPresent()) {
                CatalogSchemaTableName preAggregationTableKey = preAggregationTableKeyOpt.get();
                String schemaName = preAggregationTableKey.getSchemaTableName().getSchemaName();
                String tableName = preAggregationTableKey.getSchemaTableName().getTableName();
                String preAggregationTable = convertTable(preAggregationTableKey);

                visitedAggregationTables.put(QualifiedName.of(tableName), preAggregationTable);
                visitedAggregationTables.put(QualifiedName.of(schemaName, tableName), preAggregationTable);
                visitedAggregationTables.put(QualifiedName.of(preAggregationTableKey.getCatalogName(), schemaName, tableName), preAggregationTable);
                return new Table(
                        node.getLocation().get(),
                        QualifiedName.of(preAggregationTable),
                        qualifiedName(preAggregationTableKey));
            }

            return node;
        }

        private Optional<CatalogSchemaTableName> getPreAggregationKey(QualifiedName tableName)
        {
            try {
                if (tableName.getParts().size() == 1 && visitedAlias.contains(new Identifier(tableName.getParts().get(0)))) {
                    return Optional.empty();
                }
                return Optional.of(toCatalogSchemaTableName(sessionContext, tableName));
            }
            catch (IllegalArgumentException e) {
                return Optional.empty();
            }
        }

        protected QualifiedName getQualifiedName(Expression expression)
        {
            if (expression instanceof DereferenceExpression) {
                return DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
            }
            if (expression instanceof Identifier) {
                return QualifiedName.of(ImmutableList.of((Identifier) expression));
            }
            return null;
        }

        private String convertTable(CatalogSchemaTableName preAggregationTable)
        {
            return converter.apply(preAggregationTable)
                    .orElseThrow(() -> new GraphMDLException(GENERIC_USER_ERROR, "Table " + preAggregationTable + " is not found"));
        }
    }

    protected static QualifiedName qualifiedName(CatalogSchemaTableName table)
    {
        return QualifiedName.of(ImmutableList.of(
                new Identifier(table.getCatalogName()),
                identifier(table.getSchemaTableName().getSchemaName()),
                identifier(table.getSchemaTableName().getTableName())));
    }

    protected static Identifier identifier(String name)
    {
        if (KEYWORDS.contains(name.toUpperCase(ENGLISH))) {
            return new Identifier(name, true);
        }
        return new Identifier(name);
    }
}
