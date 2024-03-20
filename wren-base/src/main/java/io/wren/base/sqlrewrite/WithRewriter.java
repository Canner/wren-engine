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

import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class WithRewriter
        extends BaseRewriter<Void>
{
    private final List<WithQuery> withQueries;

    public WithRewriter(List<WithQuery> withQueries)
    {
        this.withQueries = requireNonNull(withQueries, "withQueries is null");
    }

    @Override
    protected Node visitQuery(Query node, Void context)
    {
        return new Query(
                node.getWith()
                        .map(with -> new With(
                                with.isRecursive(),
                                // model queries must come first since with-queries may use models
                                // and tables in with query should all be in order.
                                Stream.concat(withQueries.stream(), with.getQueries().stream())
                                        .collect(toUnmodifiableList())))
                        .or(() -> withQueries.isEmpty() ? Optional.empty() : Optional.of(new With(false, withQueries))),
                node.getQueryBody(),
                node.getOrderBy(),
                node.getOffset(),
                node.getLimit());
    }

    public static WithQuery getWithQuery(QueryDescriptor queryDescriptor)
    {
        return new WithQuery(new Identifier(queryDescriptor.getName(), true), queryDescriptor.getQuery(), Optional.empty());
    }
}
