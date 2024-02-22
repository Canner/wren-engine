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

package io.accio.main.wireprotocol;

import io.accio.base.AccioMDL;
import io.accio.base.pgcatalog.function.PgMetastoreFunctionRegistry;
import io.accio.base.sqlrewrite.analyzer.ExpressionTypeAnalyzer;
import io.accio.base.sqlrewrite.analyzer.Scope;
import io.accio.base.type.PGType;
import io.airlift.log.Logger;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Table;

import java.util.ArrayList;
import java.util.List;

import static io.accio.base.sqlrewrite.Utils.parseQuery;
import static java.util.stream.Collectors.toList;

public class PgQueryAnalyzer
        extends DefaultTraversalVisitor<Void>
{
    private static final Logger LOG = Logger.get(PgQueryAnalyzer.class);

    public static boolean isMetadataQuery(String sql, AccioMDL mdl)
    {
        try {
            PgQueryAnalyzer analyzer = new PgQueryAnalyzer(mdl);
            analyzer.process(parseQuery(sql));
            return !analyzer.visitedPgTable.isEmpty() ||
                    !analyzer.visitedPgFunction.isEmpty() ||
                    analyzer.useOidType;
        }
        catch (Exception e) {
            LOG.debug(e, "Failed to analyze query: %s", sql);
            return false;
        }
    }

    private final List<String> visitedPgTable = new ArrayList<>();
    private final List<String> visitedPgFunction = new ArrayList<>();
    private boolean useOidType;
    private final PgMetastoreFunctionRegistry pgMetastoreFunctionRegistry = new PgMetastoreFunctionRegistry();
    private final AccioMDL mdl;

    private PgQueryAnalyzer(AccioMDL mdl)
    {
        this.mdl = mdl;
    }

    @Override
    protected Void visitTable(Table node, Void context)
    {
        if (node.getName().toString().startsWith("pg_")) {
            visitedPgTable.add(node.getName().toString());
        }
        return super.visitTable(node, context);
    }

    @Override
    protected Void visitFunctionCall(FunctionCall node, Void context)
    {
        if (isVisitedPgFunction(node)) {
            visitedPgFunction.add(node.getName().toString());
        }

        return super.visitFunctionCall(node, context);
    }

    private boolean isVisitedPgFunction(FunctionCall node)
    {
        if (node.getName().hasPrefix(QualifiedName.of("pg_catalog")) ||
                node.getName().hasPrefix(QualifiedName.of("information_schema"))) {
            return true;
        }

        Scope scope = Scope.builder().build();
        List<PGType<?>> pgTypes = node.getArguments().stream().map(arg -> ExpressionTypeAnalyzer.analyze(mdl, scope, arg)).collect(toList());
        return pgMetastoreFunctionRegistry.getFunction(node.getName().getSuffix(), pgTypes).isPresent();
    }

    @Override
    protected Void visitCast(Cast node, Void context)
    {
        if (node.getType().toString().startsWith("reg")) {
            useOidType = true;
        }
        return super.visitCast(node, context);
    }

    @Override
    protected Void visitGenericLiteral(GenericLiteral node, Void context)
    {
        if (node.getType().startsWith("reg")) {
            useOidType = true;
        }
        return super.visitGenericLiteral(node, context);
    }
}
