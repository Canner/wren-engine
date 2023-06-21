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

import io.accio.base.dto.Relationship;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ExpressionAnalysis
{
    private final Expression expression;
    private final Map<NodeRef<Expression>, Expression> relationshipFieldRewrites;
    private final Set<String> relationshipCTENames;
    private final Set<Relationship> relationships;

    public ExpressionAnalysis(
            Expression expression,
            Map<NodeRef<Expression>, Expression> relationshipFields,
            Set<String> relationshipCTENames,
            Set<Relationship> relationships)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.relationshipFieldRewrites = requireNonNull(relationshipFields, "relationshipFields is null");
        this.relationshipCTENames = requireNonNull(relationshipCTENames, "relationshipCTENames is null");
        this.relationships = requireNonNull(relationships, "relationships is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public Map<NodeRef<Expression>, Expression> getRelationshipFieldRewrites()
    {
        return relationshipFieldRewrites;
    }

    public Set<String> getRelationshipCTENames()
    {
        return relationshipCTENames;
    }

    public Set<Relationship> getRelationships()
    {
        return relationships;
    }
}
