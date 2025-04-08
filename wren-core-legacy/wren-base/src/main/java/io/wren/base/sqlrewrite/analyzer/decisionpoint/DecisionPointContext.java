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

package io.wren.base.sqlrewrite.analyzer.decisionpoint;

import io.wren.base.sqlrewrite.analyzer.Scope;

import java.util.Optional;

public class DecisionPointContext
{
    private final QueryAnalysis.Builder builder;
    private final Scope scope;
    private final boolean isSubqueryOrCte;

    public static DecisionPointContext withSubqueryOrCte(DecisionPointContext decisionPointContext, boolean isSubqueryOrCte)
    {
        return Optional.ofNullable(decisionPointContext)
                .map(c -> new DecisionPointContext(c.builder, c.scope, isSubqueryOrCte))
                .orElse(new DecisionPointContext(null, null, isSubqueryOrCte));
    }

    /**
     * Because the context could be null, use this method to get the value of isSubqueryOrCte safely.
     */
    public static boolean isSubqueryOrCte(DecisionPointContext decisionPointContext)
    {
        return Optional.ofNullable(decisionPointContext).map(c -> c.isSubqueryOrCte).orElse(false);
    }

    public DecisionPointContext(QueryAnalysis.Builder builder, Scope scope, boolean isSubqueryOrCte)
    {
        this.builder = builder;
        this.scope = scope;
        this.isSubqueryOrCte = isSubqueryOrCte;
    }

    public QueryAnalysis.Builder getBuilder()
    {
        return builder;
    }

    public Scope getScope()
    {
        return scope;
    }
}
