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

package io.cml.ml;

import io.cml.wireprotocol.BaseRewriteVisitor;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.cml.ml.GraphML.FieldDefinition;
import static io.cml.ml.GraphML.Model;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class MLSqlRewrite
{
    private MLSqlRewrite() {}

    public static Statement rewrite(Statement statement, GraphMLStore graphMLStore)
    {
        return (Statement) new MLSqlRewrite.Visitor(graphMLStore).process(statement);
    }

    private static class Visitor
            extends BaseRewriteVisitor
    {
        private final GraphMLStore graphMLStore;

        public Visitor(GraphMLStore graphMLStore) {
            this.graphMLStore = requireNonNull(graphMLStore, "modelStore is null");
        }

        @Override
        protected Node visitQuerySpecification(QuerySpecification querySpecification, Void context)
        {
            Optional<Model> optModel = querySpecification.getFrom()
                    .filter(from -> from instanceof Table)
                    .flatMap(from -> graphMLStore.getModel(((Table) from).getName().toString()));

            if (optModel.isEmpty()) {
                return super.visitQuerySpecification(querySpecification, context);
            }
            Model model = optModel.get();
            List<FieldDefinition> fieldDefinitions = model.columns;
            Select select = querySpecification.getSelect();
            List<SelectItem> selectItems = new ArrayList<>();
            for (SelectItem item : select.getSelectItems()) {
                if (item instanceof AllColumns) {
                    selectItems = fieldDefinitions.stream()
                            .map(fieldDefinition -> new SingleColumn(new Identifier(fieldDefinition.name)))
                            .collect(toUnmodifiableList());
                }
                else if (item instanceof SingleColumn) {
                    SingleColumn singleColumn = (SingleColumn) item;
                    Expression singleColumnExpression = singleColumn.getExpression();
                    if (singleColumnExpression instanceof Identifier) {
                        Identifier identifier = (Identifier) singleColumnExpression;
                        FieldDefinition fieldDefinition = fieldDefinitions.stream()
                                .filter(field -> field.name.equals(identifier.getValue())).findAny()
                                .orElseThrow(() -> new IllegalArgumentException(format("Access Denied, column %s is not in model %s", identifier, model.name)));
                        singleColumn = new SingleColumn(new Identifier(fieldDefinition.name), singleColumn.getAlias());
                    }
                    else if (singleColumnExpression instanceof DereferenceExpression) {
                        DereferenceExpression dereferenceExpression = (DereferenceExpression) singleColumnExpression;
                        FieldDefinition fieldDefinition = fieldDefinitions.stream()
                                .filter(field -> field.name.equals(dereferenceExpression.getField().getValue())).findAny()
                                .orElseThrow(() -> new IllegalArgumentException(
                                        format("Access Denied, column %s is not in model %s", dereferenceExpression.getField().getValue(), model.name)));
                        singleColumn = new SingleColumn(new DereferenceExpression(dereferenceExpression.getBase(), new Identifier(fieldDefinition.name)), singleColumn.getAlias());
                    }
                    else {
                        singleColumn = visitAndCast(singleColumn);
                    }
                    selectItems.add(singleColumn);
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            Statement modelStmt = new SqlParser().createStatement(model.refSql, new ParsingOptions(AS_DECIMAL));
            checkArgument(modelStmt instanceof Query, "model sql should be query");

            return new QuerySpecification(
                    new Select(select.isDistinct(), selectItems),
                    Optional.of(new TableSubquery((Query) modelStmt)),
                    querySpecification.getWhere().map(this::visitAndCast),
                    querySpecification.getGroupBy().map(this::visitAndCast),
                    querySpecification.getHaving().map(this::visitAndCast),
                    visitNodes(querySpecification.getWindows()),
                    querySpecification.getOrderBy().map(this::visitAndCast),
                    querySpecification.getOffset().map(this::visitAndCast),
                    querySpecification.getLimit().map(this::visitAndCast)
            );
        }
    }
}
