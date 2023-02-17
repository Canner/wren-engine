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

package io.graphmdl.main.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.graphmdl.base.CatalogSchemaTableName;
import io.graphmdl.base.metadata.SchemaTableName;
import io.graphmdl.main.pgcatalog.regtype.RegObjectFactory;
import io.graphmdl.main.wireprotocol.BaseRewriteVisitor;
import io.trino.sql.parser.SqlBaseLexer;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.graphmdl.main.pgcatalog.table.PgCatalogTableUtils.INFORMATION_SCHEMA;
import static io.graphmdl.main.sql.PgOidTypeTableInfo.REGCLASS;
import static io.graphmdl.main.sql.PgOidTypeTableInfo.REGPROC;
import static io.trino.sql.QueryUtil.functionCall;
import static java.util.Locale.ENGLISH;
import static java.util.Locale.ROOT;

public class PostgreSqlRewrite
{
    private static final String PGCATALOG_TABLE_PREFIX = "pg_";
    private static final String PGCATALOG = "pg_catalog";
    private static final List<String> SYSTEM_SCHEMAS = List.of(INFORMATION_SCHEMA, PGCATALOG);
    private static final List<String> NON_RESERVED = List.of("session_user", "user");

    private static final Set<String> KEYWORDS = ImmutableSet.copyOf(SqlBaseLexer.ruleNames);

    public Statement rewrite(RegObjectFactory regObjectFactory, String defaultCatalog, Statement statement)
    {
        return (Statement) new Visitor(new RegObjectInterpreter(regObjectFactory), defaultCatalog).process(statement);
    }

    private static class Visitor
            extends BaseRewriteVisitor<Visitor.RewriteContext>
    {
        private final Map<Identifier, Expression> selectItemsMap = new HashMap<>();

        private final RegObjectInterpreter regObjectInterpreter;

        private final String defaultCatalog;

        public Visitor(RegObjectInterpreter regObjectInterpreter, String defaultCatalog)
        {
            this.regObjectInterpreter = regObjectInterpreter;
            this.defaultCatalog = defaultCatalog;
        }

        @Override
        protected Node visitSelect(Select node, RewriteContext context)
        {
            if (node.getLocation().isPresent()) {
                return new Select(
                        node.getLocation().get(),
                        node.isDistinct(),
                        visitNodes(node.getSelectItems()));
            }
            return new Select(
                    node.isDistinct(),
                    visitNodes(node.getSelectItems()));
        }

        @Override
        protected Node visitSimpleGroupBy(SimpleGroupBy node, RewriteContext context)
        {
            Map<Identifier, Expression> aliasedToExpressionMap = context.getAliasedNameToExpressionMap();
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            node.getExpressions().forEach(expression -> {
                if (isColumnAlias(expression, aliasedToExpressionMap)) {
                    builder.add(aliasedToExpressionMap.get(expression));
                    return;
                }
                builder.add(expression);
            });
            List<Expression> expressions = builder.build();
            return (node.getLocation().isPresent()) ?
                    new SimpleGroupBy(node.getLocation().get(), expressions) :
                    new SimpleGroupBy(expressions);
        }

        private boolean isColumnAlias(Expression expression, Map<Identifier, Expression> aliasedToExpressionMap)
        {
            return expression instanceof Identifier && aliasedToExpressionMap.containsKey(expression);
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification node, RewriteContext context)
        {
            // Relations should be visited first for alias.
            Optional<Relation> from = node.getFrom().map(this::visitAndCast);
            RewriteContext rewriteContext = analyzeSelectItem(node.getSelect());
            if (node.getLocation().isPresent()) {
                return new QuerySpecification(
                        node.getLocation().get(),
                        visitAndCast(node.getSelect()),
                        from,
                        node.getWhere().map(this::visitAndCast),
                        node.getGroupBy().map(groupBy -> (GroupBy) process(groupBy, rewriteContext)),
                        node.getHaving().map(this::visitAndCast),
                        visitNodes(node.getWindows()),
                        node.getOrderBy().map(this::visitAndCast),
                        node.getOffset(),
                        node.getLimit());
            }
            return new QuerySpecification(
                    visitAndCast(node.getSelect()),
                    from,
                    node.getWhere().map(this::visitAndCast),
                    node.getGroupBy().map(this::visitAndCast),
                    node.getHaving().map(this::visitAndCast),
                    visitNodes(node.getWindows()),
                    node.getOrderBy().map(this::visitAndCast),
                    node.getOffset(),
                    node.getLimit());
        }

        private RewriteContext analyzeSelectItem(Select select)
        {
            ImmutableMap.Builder<Identifier, Expression> aliasedNameToExpressionMap = ImmutableMap.builder();
            for (SelectItem selectItem : select.getSelectItems()) {
                if (selectItem instanceof SingleColumn) {
                    ((SingleColumn) selectItem).getAlias()
                            .ifPresent(identifier -> aliasedNameToExpressionMap.put(identifier, ((SingleColumn) selectItem).getExpression()));
                }
            }
            return new RewriteContext(aliasedNameToExpressionMap.build());
        }

        @Override
        protected Node visitSingleColumn(SingleColumn node, RewriteContext context)
        {
            Expression expression = node.getExpression();
            // Show regObject's name when it's in a single column.
            if (expression instanceof GenericLiteral || expression instanceof Cast) {
                Optional<Object> result = regObjectInterpreter.evaluate(expression, true);
                expression = result.map(obj -> (Expression) obj).orElse(expression);
            }
            if (node.getAlias().isPresent()) {
                // TODO: store the mapping according the scope
                //  https://github.com/Canner/canner-metric-layer/issues/168
                selectItemsMap.put(node.getAlias().get(), expression);
            }

            Optional<Identifier> alias = rewriteAlias(node.getAlias(), expression);

            return (node.getLocation().isPresent()) ?
                    new SingleColumn(
                            node.getLocation().get(),
                            visitAndCast(expression),
                            alias) :
                    new SingleColumn(
                            visitAndCast(expression),
                            alias);
        }

        @Override
        protected Node visitCast(Cast node, RewriteContext context)
        {
            String type = node.getType().toString().toUpperCase(ROOT);
            if (type.equals(REGPROC.name()) || type.equals(REGCLASS.name())) {
                Optional<Object> result = regObjectInterpreter.evaluate(node, false);
                return result.map(obj -> (Expression) obj).orElse(node);
            }
            return node;
        }

        @Override
        protected Node visitExpression(Expression expression, RewriteContext context)
        {
            Optional<Object> result = regObjectInterpreter.evaluate(expression, false);
            return result.map(o -> (Node) o).orElse(expression);
        }

        private Optional<Identifier> rewriteAlias(Optional<Identifier> alias, Expression expression)
        {
            if (alias.isPresent()) {
                return alias;
            }
            else if (expression instanceof FunctionCall) {
                FunctionCall functionCallExpression = (FunctionCall) expression;
                return Optional.of(new Identifier(functionCallExpression.getName().getSuffix()));
            }
            return Optional.empty();
        }

        @Override
        protected Node visitJoin(Join node, RewriteContext context)
        {
            if (node.getLocation().isPresent()) {
                return new Join(
                        node.getLocation().get(),
                        node.getType(),
                        visitAndCast(node.getLeft()),
                        visitAndCast(node.getRight()),
                        node.getCriteria().map(this::visitJoinCriteria));
            }
            return new Join(
                    node.getType(),
                    visitAndCast(node.getLeft()),
                    visitAndCast(node.getRight()),
                    node.getCriteria().map(this::visitJoinCriteria));
        }

        @Override
        protected Node visitTable(Table node, RewriteContext context)
        {
            if (isBelongPgCatalog(node.getName().getParts())) {
                if (node.getLocation().isPresent()) {
                    return new Table(
                            node.getLocation().get(),
                            qualifiedName(toDefaultCatalogPgCatalogSchemaTableName(node.getName().getParts())));
                }
                return new Table(qualifiedName(toDefaultCatalogPgCatalogSchemaTableName(node.getName().getParts())));
            }
            if (node.getLocation().isPresent()) {
                return new Table(
                        node.getLocation().get(),
                        node.getName());
            }
            return new Table(node.getName());
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, RewriteContext context)
        {
            QualifiedName name = getQualifiedName(node.getBase());
            Expression base;
            if (name == null) {
                base = visitAndCast(node.getBase());
            }
            else if (isBelongPgCatalog(name.getParts())) {
                base = DereferenceExpression.from(qualifiedName(toDefaultCatalogPgCatalogSchemaTableName(name.getParts())));
            }
            else {
                base = DereferenceExpression.from(name);
            }
            if (node.getLocation().isPresent()) {
                return new DereferenceExpression(
                        node.getLocation().get(),
                        base,
                        node.getField());
            }
            return new DereferenceExpression(
                    base,
                    node.getField());
        }

        @Override
        protected Node visitFunctionCall(FunctionCall node, RewriteContext context)
        {
            switch (node.getName().getSuffix()) {
                case "version":
                    return functionCall("pg_version");
                case "to_date":
                    return functionCall("pg_to_date");
            }
            return new FunctionCall(
                    node.getLocation(),
                    removeNamespace(node.getName()),
                    node.getWindow().map(this::visitAndCast),
                    node.getFilter().map(this::visitAndCast),
                    node.getOrderBy().map(this::visitAndCast),
                    node.isDistinct(),
                    node.getNullTreatment(),
                    Optional.empty(),
                    visitNodes(node.getArguments()));
        }

        @Override
        protected Node visitCurrentUser(CurrentUser node, RewriteContext context)
        {
            return functionCall("$current_user");
        }

        @Override
        protected Node visitIdentifier(Identifier node, RewriteContext context)
        {
            if (List.of("session_user", "user").contains(node.getValue().toLowerCase(ROOT))) {
                return functionCall("$current_user");
            }
            return super.visitIdentifier(node, context);
        }

        @Override
        protected Node visitLikePredicate(LikePredicate node, RewriteContext context)
        {
            if (node.getEscape().isPresent()) {
                return super.visitLikePredicate(node, context);
            }

            StringLiteral backslashEscape = new StringLiteral("\\");
            if (node.getLocation().isEmpty()) {
                return super.visitLikePredicate(new LikePredicate(node.getValue(), node.getPattern(), Optional.of(backslashEscape)), context);
            }
            return super.visitLikePredicate(new LikePredicate(node.getLocation().get(), node.getValue(), node.getPattern(), Optional.of(backslashEscape)), context);
        }

        private static boolean isNonReservedLexer(String value)
        {
            return NON_RESERVED.contains(value);
        }

        private CatalogSchemaTableName toDefaultCatalogPgCatalogSchemaTableName(List<String> parts)
        {
            return new CatalogSchemaTableName(defaultCatalog, new SchemaTableName(PGCATALOG, parts.get(parts.size() - 1)));
        }

        private static QualifiedName removeNamespace(QualifiedName name)
        {
            if (SYSTEM_SCHEMAS.contains(name.getOriginalParts().get(0).getValue())) {
                return QualifiedName.of(name.getSuffix());
            }
            return name;
        }

        protected JoinCriteria visitJoinCriteria(JoinCriteria joinCriteria)
        {
            if (joinCriteria instanceof JoinOn) {
                JoinOn joinOn = (JoinOn) joinCriteria;
                return new JoinOn(visitAndCast(joinOn.getExpression()));
            }

            return joinCriteria;
        }

        private boolean isBelongPgCatalog(List<String> parts)
        {
            // sql submitted by pg jdbc will only like `pg_type` and `pg_catalog.pg_type`.
            if (parts.size() == 1) {
                return parts.get(0).startsWith(PGCATALOG_TABLE_PREFIX);
            }
            else if (parts.size() == 2) {
                return parts.get(0).equals(PGCATALOG);
            }
            return false;
        }

        protected <T extends Node> T visitAndCast(T node)
        {
            return (T) process(node);
        }

        protected <T extends Window> T visitAndCast(T window)
        {
            Node node = null;
            if (window instanceof WindowSpecification) {
                node = (WindowSpecification) window;
            }
            else if (window instanceof WindowReference) {
                node = (WindowReference) window;
            }
            return (T) process(node);
        }

        protected static QualifiedName getQualifiedName(Expression expression)
        {
            if (expression instanceof DereferenceExpression) {
                return DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
            }
            if (expression instanceof Identifier) {
                return QualifiedName.of(ImmutableList.of((Identifier) expression));
            }
            return null;
        }

        protected static QualifiedName qualifiedName(CatalogSchemaTableName table)
        {
            return QualifiedName.of(ImmutableList.of(
                    identifier(table.getCatalogName()),
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

        static class RewriteContext
        {
            private final Map<Identifier, Expression> aliasedNameToExpressionMap;

            public RewriteContext(Map<Identifier, Expression> aliasedNameToExpressionMap)
            {
                this.aliasedNameToExpressionMap = aliasedNameToExpressionMap;
            }

            public Map<Identifier, Expression> getAliasedNameToExpressionMap()
            {
                return aliasedNameToExpressionMap;
            }
        }
    }
}
