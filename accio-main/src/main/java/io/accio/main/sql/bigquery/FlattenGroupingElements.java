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

package io.accio.main.sql.bigquery;

import com.google.common.collect.ImmutableList;
import io.accio.main.metadata.Metadata;
import io.accio.main.sql.SqlRewrite;
import io.accio.sqlrewrite.BaseRewriter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.SimpleGroupBy;

import java.util.List;

/**
 * BigQuery doesn't allow group-by key as a struct like (c1, c2).
 * Flatten all grouping elements in this class.
 * GROUP BY (c1, c2) -> GROUP BY c1, c2
 */
public class FlattenGroupingElements
        implements SqlRewrite
{
    public static final FlattenGroupingElements INSTANCE = new FlattenGroupingElements();

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new FlattenGroupingElementsRewriter().process(node, null);
    }

    public static class FlattenGroupingElementsRewriter
            extends BaseRewriter<Void>
    {
        protected Node visitGroupBy(GroupBy node, Void context)
        {
            ImmutableList.Builder<GroupingElement> builder = ImmutableList.builder();
            node.getGroupingElements().forEach(groupingElement -> {
                if (groupingElement instanceof SimpleGroupBy) {
                    rewriteSimpleGroupBy((SimpleGroupBy) groupingElement, builder);
                }
                else {
                    builder.add(groupingElement);
                }
            });
            if (node.getLocation().isPresent()) {
                return new GroupBy(
                        node.getLocation().get(),
                        node.isDistinct(),
                        builder.build());
            }
            return new GroupBy(
                    node.isDistinct(),
                    builder.build());
        }

        private static void rewriteSimpleGroupBy(SimpleGroupBy node, ImmutableList.Builder<GroupingElement> groupingElementBuilder)
        {
            for (Expression expression : node.getExpressions()) {
                groupingElementBuilder.add(new SimpleGroupBy(List.of(expression)));
            }
        }
    }
}
