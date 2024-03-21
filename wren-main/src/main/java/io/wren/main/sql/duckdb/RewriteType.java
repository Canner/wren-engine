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

package io.wren.main.sql.duckdb;

import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.TypeParameter;
import io.wren.base.sqlrewrite.BaseRewriter;
import io.wren.base.type.PGArray;
import io.wren.main.metadata.Metadata;
import io.wren.main.sql.SqlRewrite;

import java.util.List;
import java.util.Optional;

import static io.wren.base.client.duckdb.DuckdbTypes.toDuckdbType;
import static io.wren.base.type.PGArray.allArray;

public class RewriteType
        implements SqlRewrite
{
    public static final RewriteType INSTANCE = new RewriteType();

    private RewriteType() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        return new RewriteArrayRewriter().process(node, null);
    }

    private static class RewriteArrayRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitGenericDataType(GenericDataType genericDataType, Void context)
        {
            Optional<NodeLocation> nodeLocation = genericDataType.getLocation();
            String typeName = genericDataType.getName().getCanonicalValue();
            if (typeName.startsWith("_")) {
                PGArray pgArray = getPgArrayType(typeName);
                return new GenericDataType(nodeLocation, new Identifier("ARRAY"),
                        List.of(new TypeParameter(new GenericDataType(nodeLocation, new Identifier(toDuckdbType(pgArray.getInnerType()).getName()), List.of()))));
            }
            return genericDataType;
        }

        private static PGArray getPgArrayType(String arrayTypeName)
        {
            for (PGArray pgArray : allArray()) {
                if (arrayTypeName.equalsIgnoreCase(pgArray.typName())) {
                    return pgArray;
                }
            }
            throw new UnsupportedOperationException("Unsupported array type: " + arrayTypeName);
        }
    }
}
