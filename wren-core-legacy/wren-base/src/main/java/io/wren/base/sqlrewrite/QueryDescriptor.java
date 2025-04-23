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
import io.wren.base.WrenMDL;
import io.wren.base.dto.CumulativeMetric;
import io.wren.base.dto.Metric;
import io.wren.base.dto.Model;
import io.wren.base.dto.View;

import java.util.Optional;
import java.util.Set;

public interface QueryDescriptor
{
    String getName();

    Set<String> getRequiredObjects();

    Query getQuery();

    static QueryDescriptor of(String name, AnalyzedMDL analyzedMDL, SessionContext sessionContext)
    {
        WrenMDL mdl = analyzedMDL.getWrenMDL();
        Optional<Model> model = mdl.getModel(name);
        if (model.isPresent()) {
            return RelationInfo.get(model.get(), mdl);
        }
        Optional<Metric> metric = mdl.getMetric(name);
        if (metric.isPresent()) {
            return RelationInfo.get(metric.get(), mdl);
        }
        Optional<CumulativeMetric> cumulativeMetric = mdl.getCumulativeMetric(name);
        if (cumulativeMetric.isPresent()) {
            return CumulativeMetricInfo.get(cumulativeMetric.get(), mdl);
        }
        Optional<View> view = mdl.getView(name);
        if (view.isPresent()) {
            return ViewInfo.get(view.get(), analyzedMDL, sessionContext);
        }
        if (name.equals(DateSpineInfo.NAME)) {
            return DateSpineInfo.get(mdl.getDateSpine());
        }
        throw new IllegalArgumentException(name + " not found in wren mdl");
    }
}
