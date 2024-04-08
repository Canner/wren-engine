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

package io.wren.main.wireprotocol;

import com.google.inject.Inject;
import io.trino.sql.tree.Statement;
import io.wren.cache.ExtraRewriter;
import io.wren.main.metadata.Metadata;
import io.wren.main.pgcatalog.regtype.RegObjectFactory;
import io.wren.main.sql.PostgreSqlRewrite;

import static java.util.Objects.requireNonNull;

public class PgWireProtocolExtraRewriter
        implements ExtraRewriter
{
    private final RegObjectFactory regObjectFactory;
    private final Metadata metadata;

    @Inject
    public PgWireProtocolExtraRewriter(RegObjectFactory regObjectFactory, Metadata metadata)
    {
        this.regObjectFactory = requireNonNull(regObjectFactory, "regObjectFactory is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Statement rewrite(Statement statement)
    {
        return PostgreSqlRewrite.rewrite(regObjectFactory, metadata.getDefaultCatalog(), metadata.getPgCatalogName(), statement);
    }
}
