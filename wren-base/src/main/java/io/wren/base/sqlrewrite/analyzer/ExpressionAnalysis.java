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

package io.wren.base.sqlrewrite.analyzer;

import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ExpressionAnalysis
{
    private final List<Field> collectedFields;
    private final Map<NodeRef<Expression>, Field> referencedFields;
    private final List<ComparisonExpression> predicates;
    // For `count(*)` expression, we should generate the specific CTE for it.
    private final boolean requireRelation;

    public ExpressionAnalysis(Map<NodeRef<Expression>, Field> referenceFields, List<ComparisonExpression> predicates, boolean requireRelation)
    {
        this.referencedFields = requireNonNull(referenceFields);
        this.collectedFields = referenceFields.values().stream().collect(toImmutableList());
        this.predicates = requireNonNull(predicates);
        this.requireRelation = requireRelation;
    }

    public List<Field> getCollectedFields()
    {
        return collectedFields;
    }

    public Map<NodeRef<Expression>, Field> getReferencedFields()
    {
        return referencedFields;
    }

    public List<ComparisonExpression> getPredicates()
    {
        return predicates;
    }

    public boolean isRequireRelation()
    {
        return requireRelation;
    }
}
