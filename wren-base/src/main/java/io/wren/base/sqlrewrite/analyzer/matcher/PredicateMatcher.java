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

package io.wren.base.sqlrewrite.analyzer.matcher;

import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.Node;

public class PredicateMatcher
        implements Matcher
{
    public static final PredicateMatcher PREDICATE_MATCHER = new PredicateMatcher();

    private PredicateMatcher() {}

    @Override
    public boolean shapeMatches(Node node)
    {
        if (!(node instanceof ComparisonExpression)) {
            return false;
        }

        Expression left = ((ComparisonExpression) node).getLeft();
        Expression right = ((ComparisonExpression) node).getRight();

        return (left instanceof DereferenceExpression || left instanceof Identifier) &&
                (right instanceof Literal || right instanceof DereferenceExpression || right instanceof Identifier);
    }
}
