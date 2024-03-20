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
import io.wren.base.WrenMDL;
import io.wren.base.dto.CumulativeMetric;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class CumulativeMetricInfo
        implements QueryDescriptor
{
    private final String name;
    private final Set<String> requiredObjects;
    private final Query query;

    public static CumulativeMetricInfo get(CumulativeMetric metric, WrenMDL mdl)
    {
        return new CumulativeMetricInfo(
                metric.getName(),
                Set.of(metric.getBaseObject(), DateSpineInfo.NAME),
                Utils.parseCumulativeMetricSql(metric, mdl));
    }

    private CumulativeMetricInfo(String name, Set<String> requiredObjects, Query query)
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
