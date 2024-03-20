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

package io.wren.main.sql.bigquery;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Node;
import io.wren.base.sqlrewrite.BaseRewriter;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;

import java.util.Optional;

public class RemoveParameterInTypesInCast
        implements SqlRewrite
{
    public static final RemoveParameterInTypesInCast INSTANCE = new RemoveParameterInTypesInCast();

    private RemoveParameterInTypesInCast() {}

    public Node rewrite(Node node, Metadata metadata)
    {
        return new RemoveParameterInTypesInCastRewriter().process(node);
    }

    private static class RemoveParameterInTypesInCastRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitCast(Cast node, Void context)
        {
            DataType dataType = node.getType();
            if (node.getType() instanceof GenericDataType
                    && !((GenericDataType) node.getType()).getName().getCanonicalValue().equals("ARRAY")) {
                GenericDataType genericDataType = (GenericDataType) node.getType();
                dataType = new GenericDataType(Optional.empty(), genericDataType.getName(), ImmutableList.of());
            }
            else if (node.getType() instanceof DateTimeDataType) {
                DateTimeDataType dateTimeDataType = (DateTimeDataType) node.getType();
                dataType = new DateTimeDataType(dateTimeDataType.getLocation(), dateTimeDataType.getType(), dateTimeDataType.isWithTimeZone(), Optional.empty());
            }
            return new Cast(node.getExpression(), dataType);
        }
    }
}
