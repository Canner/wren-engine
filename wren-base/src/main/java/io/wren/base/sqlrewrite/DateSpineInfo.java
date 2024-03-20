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
import io.wren.base.dto.DateSpine;

import java.util.Set;

import static io.wren.base.sqlrewrite.Utils.createDateSpineQuery;
import static java.util.Objects.requireNonNull;

public class DateSpineInfo
        implements QueryDescriptor
{
    public static final String NAME = "date_spine";

    private final Query query;

    public static DateSpineInfo get(DateSpine dateSpine)
    {
        return new DateSpineInfo(dateSpine);
    }

    private DateSpineInfo(DateSpine dateSpine)
    {
        this.query = createDateSpineQuery(requireNonNull(dateSpine));
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Set<String> getRequiredObjects()
    {
        return Set.of();
    }

    @Override
    public Query getQuery()
    {
        return query;
    }
}
