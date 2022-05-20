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

package io.cml.calcite;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;
import static org.apache.calcite.rel.rel2sql.SqlImplementor.POS;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;

public final class CalciteSqlNodeConverter
{
    private CalciteSqlNodeConverter() {}

    public static SqlNode convert(Node statement)
    {
        Visitor visitor = new Visitor();
        return visitor.process(statement);
    }

    // TODO: fill up all sql node convert method in this Visitor
    private static class Visitor
            extends AstVisitor<SqlNode, Void>
    {
        @Override
        public SqlNode visitQuery(Query query, Void context)
        {
            return visitQuerySpecification((QuerySpecification) query.getQueryBody(), null);
        }

        @Override
        public SqlNode visitQuerySpecification(QuerySpecification node, Void context)
        {
            return new SqlSelect(
                    POS,
                    null, // fill up this
                    (SqlNodeList) visitNode(node.getSelect()),
                    node.getFrom().map(this::visitNode).orElse(null),
                    node.getWhere().map(this::visitNode).orElse(null),
                    node.getGroupBy().map(groupBy -> SqlNodeList.of(POS, visitNodes(groupBy.getGroupingElements()))).orElse(null),
                    node.getHaving().map(this::visitNode).orElse(null),
                    SqlNodeList.of(POS, visitNodes(node.getWindows())),
                    node.getOrderBy().map(orderBy -> SqlNodeList.of(POS, visitNodes(orderBy.getSortItems()))).orElse(null),
                    node.getOffset().map(offset -> visitNode(offset.getRowCount())).orElse(null),
                    null,
                    null);
        }

        @Override
        public SqlNode visitSelect(Select node, Void context)
        {
            List<SqlNode> selectItems = node.getSelectItems().stream()
                    .map(selectItem -> {
                        if (selectItem instanceof SingleColumn) {
                            SingleColumn singleColumn = (SingleColumn) selectItem;
                            return SqlStdOperatorTable.AS.createCall(
                                    POS,
                                    process(singleColumn.getExpression()),
                                    process(singleColumn.getAlias().get()));
                        }
                        else if (selectItem instanceof AllColumns) {
                            return SqlNodeList.SINGLETON_STAR;
                        }
                        else {
                            throw new IllegalArgumentException("selectItem isn't an instance of SingleColumn or AllColumns");
                        }
                    })
                    .collect(toImmutableList());

            return SqlNodeList.of(POS, selectItems);
        }

        @Override
        public SqlNode visitTable(Table node, Void context)
        {
            SqlIdentifier sqlIdentifier = new SqlIdentifier(node.getName().toString(), ZERO);
            return new SqlTableRef(
                    sqlIdentifier.getParserPosition(),
                    sqlIdentifier,
                    SqlNodeList.of(ZERO, ImmutableList.of()));
        }

        @Override
        public SqlNode visitIdentifier(Identifier identifier, Void context)
        {
            return new SqlIdentifier(identifier.getValue(), POS);
        }

        @SuppressWarnings("unchecked")
        protected <T extends SqlNode> List<SqlNode> visitNodes(List<? extends Node> nodes)
        {
            return nodes.stream()
                    .map(node -> (T) process(node))
                    .collect(toList());
        }

        protected SqlNode visitNode(Node node)
        {
            return process(node);
        }
    }
}
