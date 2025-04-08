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

import io.trino.sql.tree.Query;
import io.wren.base.AnalyzedMDL;
import io.wren.base.SessionContext;
import io.wren.base.dto.View;
import io.wren.base.sqlrewrite.analyzer.Analysis;
import io.wren.base.sqlrewrite.analyzer.StatementAnalyzer;

import java.util.Set;

import static io.wren.base.sqlrewrite.Utils.parseView;
import static java.util.Objects.requireNonNull;

public class ViewInfo
        implements QueryDescriptor
{
    private final String name;
    private final Set<String> requiredObjects;
    private final Query query;

    public static ViewInfo get(View view, AnalyzedMDL analyzedMDL, SessionContext sessionContext)
    {
        Query query = parseView(view.getStatement());
        Analysis analysis = new Analysis(query);
        StatementAnalyzer.analyze(analysis, query, sessionContext, analyzedMDL.getWrenMDL());
        // sql in view can use metric rollup syntax
        query = (Query) MetricRollupRewrite.METRIC_ROLLUP_REWRITE.apply(query, sessionContext, analysis, analyzedMDL);
        return new ViewInfo(view.getName(), analysis.getWrenObjectNames(), query);
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
