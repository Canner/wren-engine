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

import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.wren.base.sqlrewrite.analyzer.ExpressionRelationshipInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.sql.tree.DereferenceExpression.getQualifiedName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class RelationshipRewriter
        extends BaseRewriter<Void>
{
    private final Map<QualifiedName, DereferenceExpression> replacements;

    public static Node rewrite(Set<ExpressionRelationshipInfo> relationshipInfos, Expression expression)
    {
        requireNonNull(relationshipInfos);
        HashMap<QualifiedName, DereferenceExpression> replacements = new HashMap<>();
        relationshipInfos.forEach(info -> replacements.put(info.getQualifiedName(), toDereferenceExpression(info)));
        return new RelationshipRewriter(replacements)
                .process(expression);
    }

    public static Node relationshipAware(Set<ExpressionRelationshipInfo> relationshipInfos, String relationshipPrefix, Expression expression)
    {
        requireNonNull(relationshipInfos);
        return new RelationshipRewriter(relationshipInfos.stream()
                .collect(toUnmodifiableMap(ExpressionRelationshipInfo::getQualifiedName, info -> getRelationshipResultAsDereferenceExpression(info, relationshipPrefix))))
                .process(expression);
    }

    public RelationshipRewriter(Map<QualifiedName, DereferenceExpression> replacements)
    {
        this.replacements = requireNonNull(replacements);
    }

    @Override
    protected Node visitDereferenceExpression(DereferenceExpression node, Void ignored)
    {
        if (node.getField().isPresent()) {
            QualifiedName qualifiedName = getQualifiedName(node);
            if (qualifiedName != null) {
                return replacements.get(qualifiedName) == null ? node : replacements.get(qualifiedName);
            }
        }
        return node;
    }

    protected static DereferenceExpression toDereferenceExpression(ExpressionRelationshipInfo expressionRelationshipInfo)
    {
        String base = expressionRelationshipInfo.getRelationships().get(expressionRelationshipInfo.getRelationships().size() - 1).getModels().get(1);
        List<Identifier> parts = new ArrayList<>();
        parts.add(new Identifier(base, true));
        expressionRelationshipInfo.getRemainingParts().stream()
                .map(part -> new Identifier(part, true))
                .forEach(parts::add);
        return (DereferenceExpression) DereferenceExpression.from(QualifiedName.of(parts));
    }

    protected static DereferenceExpression getRelationshipResultAsDereferenceExpression(ExpressionRelationshipInfo expressionRelationshipInfo, String relationablePrefix)
    {
        // The relationshipFieldName is the name of the relationship field in the relationship model with `relationablePrefix`.
        List<Identifier> parts = new ArrayList<>();
        parts.add(new Identifier(relationablePrefix, true));
        expressionRelationshipInfo.getRemainingParts().stream()
                .map(part -> new Identifier(part, true))
                .forEach(parts::add);
        return (DereferenceExpression) DereferenceExpression.from(QualifiedName.of(parts));
    }
}
