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

package io.graphmdl.main.connector.postgres;

import io.graphmdl.preaggregation.PathInfo;
import io.graphmdl.preaggregation.PreAggregationService;

import java.util.Optional;

public class PostgresPreAggregationService
        implements PreAggregationService
{
    @Override
    public Optional<PathInfo> createPreAggregation(String catalog, String schema, String name, String statement)
    {
        return Optional.empty();
    }

    @Override
    public void deleteTarget(PathInfo pathInfo) {}
}
