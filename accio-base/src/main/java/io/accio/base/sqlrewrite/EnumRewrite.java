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

package io.accio.base.sqlrewrite;

import io.accio.base.AccioMDL;
import io.accio.base.SessionContext;
import io.accio.base.dto.EnumDefinition;
import io.accio.base.dto.EnumValue;
import io.accio.base.sqlrewrite.analyzer.Analysis;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;

import java.util.Optional;

import static java.lang.String.format;

public class EnumRewrite
        implements AccioRule
{
    public static final EnumRewrite ENUM_REWRITE = new EnumRewrite();

    private EnumRewrite() {}

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, AccioMDL accioMDL)
    {
        return apply(root, sessionContext, null, accioMDL);
    }

    @Override
    public Statement apply(Statement root, SessionContext sessionContext, Analysis analysis, AccioMDL accioMDL)
    {
        return (Statement) new Rewriter(accioMDL).process(root);
    }

    private static class Rewriter
            extends BaseRewriter<Void>
    {
        private final AccioMDL accioMDL;

        Rewriter(AccioMDL accioMDL)
        {
            this.accioMDL = accioMDL;
        }

        @Override
        protected Node visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            Expression newNode = rewriteEnumIfNeed(node);
            if (newNode != node) {
                return newNode;
            }
            return new DereferenceExpression(node.getLocation(), (Expression) process(node.getBase()), node.getField());
        }

        private Expression rewriteEnumIfNeed(DereferenceExpression node)
        {
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);
            if (qualifiedName == null || qualifiedName.getOriginalParts().size() != 2) {
                return node;
            }

            String enumName = qualifiedName.getOriginalParts().get(0).getValue();
            Optional<EnumDefinition> enumDefinitionOptional = accioMDL.getEnum(enumName);
            if (enumDefinitionOptional.isEmpty()) {
                return node;
            }

            return enumDefinitionOptional.get().valueOf(qualifiedName.getOriginalParts().get(1).getValue())
                    .map(EnumValue::getValue)
                    .map(StringLiteral::new)
                    .orElseThrow(() -> new IllegalArgumentException(format("Enum value '%s' not found in enum '%s'", qualifiedName.getParts().get(1), qualifiedName.getParts().get(0))));
        }
    }
}
