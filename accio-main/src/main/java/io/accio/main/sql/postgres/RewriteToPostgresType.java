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

package io.accio.main.sql.postgres;

import io.accio.base.sqlrewrite.BaseRewriter;
import io.accio.main.metadata.Metadata;
import io.accio.main.sql.SqlRewrite;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.StringLiteral;

import java.util.List;
import java.util.Optional;

public class RewriteToPostgresType
        implements SqlRewrite
{
    public static final RewriteToPostgresType INSTANCE = new RewriteToPostgresType();

    private RewriteToPostgresType() {}

    @Override
    public Node rewrite(Node node, Metadata metadata)
    {
        RewriteToPostgresTypeRewriter rewriter = new RewriteToPostgresTypeRewriter();
        return rewriter.process(node);
    }

    private static class RewriteToPostgresTypeRewriter
            extends BaseRewriter<Void>
    {
        @Override
        protected Node visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            // SqlBase use X'[hex string]' to represent binary data, but PostgreSQL uses the following format to represent binary data: '\x[hex string]'
            // To overcome this limitation, we convert the query to CAST('\x[hex string]' AS BYTEA).
            return new Cast(new StringLiteral("\\x" + node.toHexString()), new GenericDataType(Optional.empty(), new Identifier("BYTEA"), List.of()));
        }
    }
}
