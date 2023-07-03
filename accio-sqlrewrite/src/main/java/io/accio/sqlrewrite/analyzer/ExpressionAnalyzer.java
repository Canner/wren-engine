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

package io.accio.sqlrewrite.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.accio.base.AccioMDL;
import io.accio.base.SessionContext;
import io.accio.base.dto.Column;
import io.accio.base.dto.Model;
import io.accio.base.dto.Relationship;
import io.accio.sqlrewrite.RelationshipCTE;
import io.accio.sqlrewrite.RelationshipCteGenerator;
import io.accio.sqlrewrite.analyzer.FunctionChainAnalyzer.ReturnContext;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SubscriptExpression;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.accio.base.Utils.checkArgument;
import static io.accio.sqlrewrite.RelationshipCteGenerator.LAMBDA_RESULT_NAME;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RelationshipOperation.access;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RsItem.Type.CTE;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RsItem.Type.REVERSE_RS;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RsItem.Type.RS;
import static io.accio.sqlrewrite.RelationshipCteGenerator.RsItem.rsItem;
import static io.trino.sql.QueryUtil.getQualifiedName;
import static io.trino.sql.QueryUtil.identifier;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ExpressionAnalyzer
{
    private ExpressionAnalyzer() {}

    private final Map<NodeRef<Expression>, Expression> relationshipFieldsRewrite = new HashMap<>();
    private final Set<String> relationshipCTENames = new HashSet<>();
    private final Set<Relationship> relationships = new HashSet<>();

    public static ExpressionAnalysis analyze(
            Expression expression,
            SessionContext sessionContext,
            AccioMDL accioMDL,
            RelationshipCteGenerator relationshipCteGenerator,
            Scope scope)
    {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer();
        return expressionAnalyzer.analyzeExpression(expression, sessionContext, accioMDL, relationshipCteGenerator, scope);
    }

    private ExpressionAnalysis analyzeExpression(
            Expression expression,
            SessionContext sessionContext,
            AccioMDL accioMDL,
            RelationshipCteGenerator relationshipCteGenerator,
            Scope scope)
    {
        new Visitor(sessionContext, accioMDL, relationshipCteGenerator, scope).process(expression);
        return new ExpressionAnalysis(expression, relationshipFieldsRewrite, relationshipCTENames, relationships);
    }

    private class Visitor
            extends AstVisitor<Void, Void>
    {
        private final SessionContext sessionContext;
        private final AccioMDL accioMDL;
        private final RelationshipCteGenerator relationshipCteGenerator;
        private final Scope scope;
        private final FunctionChainAnalyzer functionChainAnalyzer;

        public Visitor(SessionContext sessionContext, AccioMDL accioMDL, RelationshipCteGenerator relationshipCteGenerator, Scope scope)
        {
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.accioMDL = requireNonNull(accioMDL, "accioMDL is null");
            this.relationshipCteGenerator = requireNonNull(relationshipCteGenerator, "relationshipCteGenerator is null");
            this.scope = requireNonNull(scope, "scope is null");
            this.functionChainAnalyzer = FunctionChainAnalyzer.of(relationshipCteGenerator, this::registerRelationshipCTEs);
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Void ignored)
        {
            process(node.getLeft());
            process(node.getRight());
            return ignored;
        }

        @Override
        protected Void visitIsNotNullPredicate(IsNotNullPredicate node, Void ignored)
        {
            process(node.getValue());
            return ignored;
        }

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, Void ignored)
        {
            String suffix = Optional.ofNullable(getQualifiedName(node.getBase())).map(QualifiedName::getSuffix).orElse(LAMBDA_RESULT_NAME);
            registerRelationshipCTEs(node.getBase())
                    .ifPresent(info -> {
                        relationshipCTENames.add(String.join(".", info.getReplacementNameParts()));
                        relationshipFieldsRewrite.put(NodeRef.of(info.getOriginal()), new DereferenceExpression(info.getReplacement(), identifier(suffix)));
                    });
            return ignored;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Void ignored)
        {
            Optional<ReturnContext> returnContext = functionChainAnalyzer.analyze(node);
            if (returnContext.isEmpty()) {
                return null;
            }

            returnContext.get().getNodesToReplace().forEach((nodeToReplace, rsField) -> {
                // nodeToReplace is a lambda function call
                if (nodeToReplace.getNode() instanceof FunctionCall) {
                    relationshipCTENames.add(nodeToReplace.getNode().toString());
                    relationshipFieldsRewrite.put(
                            nodeToReplace,
                            DereferenceExpression.from(
                                    QualifiedName.of(
                                            List.of(
                                                    relationshipCteGenerator.getNameMapping().get(nodeToReplace.getNode().toString()),
                                                    LAMBDA_RESULT_NAME))));
                }
                // nodeToReplace is a relationship field in function
                else {
                    String cteName = String.join(".", rsField.getCteNameParts());
                    relationshipCTENames.add(cteName);
                    relationshipFieldsRewrite.put(
                            nodeToReplace,
                            DereferenceExpression.from(
                                    QualifiedName.of(
                                            List.of(
                                                    relationshipCteGenerator.getNameMapping().get(cteName),
                                                    rsField.getColumnName()))));
                }
            });
            return null;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void ignored)
        {
            registerRelationshipCTEs(node)
                    .ifPresent(info -> {
                        relationshipCTENames.add(String.join(".", info.getReplacementNameParts()));
                        relationshipFieldsRewrite.put(NodeRef.of(info.getOriginal()), info.getReplacement());
                    });
            return null;
        }

        // register needed relationship CTEs and return node replacement information
        private Optional<ReplaceNodeInfo> registerRelationshipCTEs(Expression node)
        {
            if (!scope.isTableScope()) {
                return Optional.empty();
            }

            LinkedList<Expression> elements = elements(node);
            if (elements.isEmpty()) {
                return Optional.empty();
            }

            String baseModelName;
            Expression root = elements.peekFirst();
            LinkedList<String> nameParts = new LinkedList<>();
            LinkedList<RelationshipField> chain = new LinkedList<>();
            // process the root node, root node should be either FunctionCall or Identifier, if not, relationship rewrite won't be fired
            if (root instanceof FunctionCall) {
                Optional<ReturnContext> returnContext = functionChainAnalyzer.analyze((FunctionCall) root);
                boolean functionCallNeedsReplacement = returnContext.isPresent() && returnContext.get().getNodesToReplace().size() > 0;
                if (!functionCallNeedsReplacement) {
                    return Optional.empty();
                }
                Map<NodeRef<Expression>, RelationshipField> nodesToReplace = returnContext.get().getNodesToReplace();
                checkArgument(nodesToReplace.size() == 1, "No node or multiple node to replace in function chain in DereferenceExpression chain");

                nodesToReplace.forEach((nodeToReplace, rsField) ->
                        relationshipFieldsRewrite.put(
                                nodeToReplace,
                                DereferenceExpression.from(
                                        QualifiedName.of(
                                                ImmutableList.<String>builder()
                                                        .add(relationshipCteGenerator.getNameMapping().get(nodeToReplace.getNode().toString()))
                                                        .add(LAMBDA_RESULT_NAME).build()))));
                nameParts.add(elements.pop().toString());
                baseModelName = nodesToReplace.values().iterator().next().getBaseModelName();
            }
            else if (root instanceof Identifier) {
                List<Field> modelFields = scope.getRelationType()
                        .orElseThrow(() -> new IllegalArgumentException("relation type is empty"))
                        .getFields();

                Optional<Field> relationshipField = Optional.empty();
                // process column with prefix. i.e. [TableAlias|TableName].column
                while (elements.size() > 0 && relationshipField.isEmpty()) {
                    QualifiedName current;
                    Expression element = elements.pop();
                    if (element instanceof Identifier) {
                        current = QualifiedName.of(List.of((Identifier) element));
                    }
                    else if (element instanceof DereferenceExpression) {
                        current = DereferenceExpression.getQualifiedName((DereferenceExpression) element);
                    }
                    else {
                        break;
                    }

                    relationshipField = modelFields.stream()
                            .filter(scopeField -> scopeField.canResolve(current))
                            .filter(Field::isRelationship)
                            .findAny();
                }

                if (relationshipField.isPresent()) {
                    String fieldModelName = relationshipField.get().getModelName().getSchemaTableName().getTableName();
                    String fieldTypeName = relationshipField.get().getType();
                    String fieldName = relationshipField.get().getColumnName();
                    Relationship relationship = accioMDL.getRelationship(relationshipField.get().getRelationship().orElseThrow()).orElseThrow();
                    List<String> parts = List.of(fieldModelName, relationshipField.get().getColumnName());
                    relationships.add(relationship);
                    relationshipCteGenerator.register(
                            parts,
                            access(List.of(rsItem(relationship.getName(), relationship.getModels().get(0).equals(fieldTypeName) ? REVERSE_RS : RS))));

                    baseModelName = relationshipField.get().getModelName().getSchemaTableName().getTableName();
                    nameParts.addAll(parts);
                    chain.add(new RelationshipField(nameParts, fieldModelName, fieldName, relationship, baseModelName));
                }
                else {
                    return Optional.empty();
                }
            }
            else {
                return Optional.empty();
            }

            while (elements.size() > 0) {
                Expression expression = elements.pop();
                if (expression instanceof DereferenceExpression) {
                    DereferenceExpression dereferenceExpression = (DereferenceExpression) expression;
                    RelationshipCTE cte = relationshipCteGenerator.getRelationshipCTEs().get(String.join(".", nameParts));
                    if (cte == null) {
                        return Optional.empty();
                    }

                    Identifier field = dereferenceExpression.getField().orElseThrow();
                    String modelName = cte.getTarget().getName();
                    Optional<Column> relationshipColumn = accioMDL.getModel(modelName)
                            .stream()
                            .map(Model::getColumns)
                            .flatMap(List::stream)
                            .filter(column -> column.getName().equals(field.toString()) && column.getRelationship().isPresent())
                            .findAny();

                    if (relationshipColumn.isPresent()) {
                        Relationship relationship = accioMDL.getRelationship(relationshipColumn.get().getRelationship().get())
                                .orElseThrow(() -> new IllegalArgumentException("Relationship not found"));
                        relationships.add(relationship);
                        String relationshipColumnType = relationshipColumn.get().getType();
                        nameParts.add(field.toString());
                        relationshipCteGenerator.register(
                                nameParts,
                                access(List.of(
                                        rsItem(String.join(".", nameParts.subList(0, nameParts.size() - 1)), CTE),
                                        rsItem(relationship.getName(), relationship.getModels().get(0).equals(relationshipColumnType) ? REVERSE_RS : RS))),
                                baseModelName);
                        chain.add(new RelationshipField(nameParts, modelName, relationshipColumn.get().getName(), relationship, baseModelName));
                    }
                    else {
                        return Optional.of(
                                new ReplaceNodeInfo(
                                        nameParts,
                                        dereferenceExpression.getBase(),
                                        new Identifier(
                                                relationshipCteGenerator.getNameMapping().get(String.join(".", nameParts))),
                                        chain.isEmpty() ? Optional.empty() : Optional.of(chain.getLast())));
                    }
                }
                else if (expression instanceof SubscriptExpression) {
                    SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
                    String index = subscriptExpression.getIndex().toString();
                    String cteName = String.join(".", nameParts);
                    RelationshipCTE cte = relationshipCteGenerator.getRelationshipCTEs().get(cteName);
                    if (cte == null) {
                        return Optional.empty();
                    }
                    Relationship relationship = cte.getRelationship();
                    relationships.add(relationship);
                    String lastNamePart = nameParts.removeLast();
                    nameParts.add(format("%s[%s]", lastNamePart, index));

                    relationshipCteGenerator.register(
                            nameParts,
                            access(List.of(
                                    rsItem(cteName, CTE, index),
                                    rsItem(relationship.getName(), relationship.isReverse() ? REVERSE_RS : RS))),
                            baseModelName,
                            subscriptExpression.getBase() instanceof FunctionCall ? LAMBDA_RESULT_NAME : lastNamePart);
                }
                else {
                    throw new IllegalArgumentException("Unsupported operation");
                }
            }

            return Optional.ofNullable(relationshipCteGenerator.getNameMapping().get(String.join(".", nameParts)))
                    .map(cteName ->
                            new ReplaceNodeInfo(
                                    nameParts,
                                    node,
                                    new Identifier(cteName),
                                    chain.isEmpty() ? Optional.empty() : Optional.of(chain.getLast())));
        }
    }

    private static LinkedList<Expression> elements(Expression expression)
    {
        Expression current = expression;
        LinkedList<Expression> elements = new LinkedList<>();
        while (true) {
            if (current instanceof FunctionCall || current instanceof Identifier) {
                elements.add(current);
                // in dereference expression, function call or identifier should be the root node
                break;
            }
            else if (current instanceof DereferenceExpression) {
                elements.add(current);
                current = ((DereferenceExpression) current).getBase();
            }
            else if (current instanceof SubscriptExpression) {
                elements.add(current);
                current = ((SubscriptExpression) current).getBase();
            }
            else {
                // unexpected node in dereference expression, clear everything and return
                elements.clear();
                break;
            }
        }
        return new LinkedList<>(Lists.reverse(elements));
    }

    static class RelationshipField
    {
        private final List<String> cteNameParts;
        private final String modelName;
        private final String columnName;
        private final Relationship relationship;
        private final String baseModelName;

        public RelationshipField(List<String> cteNameParts, String modelName, String columnName, Relationship relationship, String baseModelName)
        {
            this.cteNameParts = requireNonNull(cteNameParts);
            this.modelName = requireNonNull(modelName);
            this.columnName = requireNonNull(columnName);
            this.relationship = requireNonNull(relationship);
            this.baseModelName = requireNonNull(baseModelName);
        }

        public List<String> getCteNameParts()
        {
            return cteNameParts;
        }

        public String getModelName()
        {
            return modelName;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Relationship getRelationship()
        {
            return relationship;
        }

        public String getBaseModelName()
        {
            return baseModelName;
        }
    }

    static class ReplaceNodeInfo
    {
        private final List<String> replacementNameParts;
        private final Expression original;
        private final Expression replacement;
        // TODO: this is required for function call processor, find other better way to search relationship field in function call
        private final Optional<RelationshipField> lastRelationshipField;

        public ReplaceNodeInfo(
                List<String> replacementNameParts,
                Expression original,
                Expression replacement,
                Optional<RelationshipField> lastRelationshipField)
        {
            this.replacementNameParts = requireNonNull(replacementNameParts);
            this.original = requireNonNull(original);
            this.replacement = requireNonNull(replacement);
            this.lastRelationshipField = requireNonNull(lastRelationshipField);
        }

        public List<String> getReplacementNameParts()
        {
            return replacementNameParts;
        }

        public Expression getOriginal()
        {
            return original;
        }

        public Expression getReplacement()
        {
            return replacement;
        }

        public Optional<RelationshipField> getLastRelationshipField()
        {
            return lastRelationshipField;
        }
    }
}
