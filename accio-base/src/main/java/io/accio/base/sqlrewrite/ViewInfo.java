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

package io.accio.base.sqlrewrite;

import io.accio.base.AccioMDL;
import io.accio.base.SessionContext;
import io.accio.base.dto.View;
import io.accio.base.sqlrewrite.analyzer.Analysis;
import io.accio.base.sqlrewrite.analyzer.StatementAnalyzer;
import io.trino.sql.tree.Query;

import java.util.Set;

import static io.accio.base.sqlrewrite.Utils.parseView;
import static java.util.Objects.requireNonNull;

public class ViewInfo
        implements QueryDescriptor
{
    private final String name;
    private final Set<String> requiredObjects;
    private final Query query;

    public static ViewInfo get(View view, AccioMDL mdl, SessionContext sessionContext)
    {
        Query query = parseView(view.getStatement());
        Analysis analysis = StatementAnalyzer.analyze(query, sessionContext, mdl);
        // sql in view can use metric rollup syntax
        query = (Query) MetricRollupRewrite.METRIC_ROLLUP_REWRITE.apply(query, sessionContext, analysis, mdl);
        return new ViewInfo(view.getName(), analysis.getAccioObjectNames(), query);
    }

    private ViewInfo(String name, Set<String> requiredObjects, Query query)
    {
        this.name = requireNonNull(name);
        this.requiredObjects = requireNonNull(requiredObjects);
        this.query = requireNonNull(query);
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Set<String> getRequiredObjects()
    {
        return requiredObjects;
    }

    @Override
    public Query getQuery()
    {
        return query;
    }
}
