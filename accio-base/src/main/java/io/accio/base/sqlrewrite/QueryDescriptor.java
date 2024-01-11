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
import io.accio.base.dto.CumulativeMetric;
import io.accio.base.dto.Metric;
import io.accio.base.dto.Model;
import io.accio.base.dto.View;
import io.trino.sql.tree.Query;

import java.util.Optional;
import java.util.Set;

public interface QueryDescriptor
{
    String getName();

    Set<String> getRequiredObjects();

    Query getQuery();

    static QueryDescriptor of(String name, AccioMDL mdl, SessionContext sessionContext)
    {
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
            return ViewInfo.get(view.get(), mdl, sessionContext);
        }
        if (name.equals(DateSpineInfo.NAME)) {
            return DateSpineInfo.get(mdl.getDateSpine());
        }
        throw new IllegalArgumentException(name + " not found in accio mdl");
    }
}
