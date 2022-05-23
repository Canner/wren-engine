/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cml.sql;

import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilder;

import java.util.Optional;

public class CalciteRelConverter
{
    public static RelNode convert(RelOptCluster cluster, RelOptSchema relOptSchema, Statement statement)
    {
        return new Visitor(cluster, relOptSchema).process(statement);
    }

    static class Visitor
            extends AstVisitor<RelNode, Optional<RelNode>>
    {
        private final RelBuilder relBuilder;

        Visitor(RelOptCluster relOptCluster, RelOptSchema relOptSchema)
        {
            this.relBuilder = RelFactories.LOGICAL_BUILDER.create(relOptCluster, relOptSchema);
        }

        @Override
        protected RelNode visitNode(Node node, Optional<RelNode> context)
        {
            throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
        }

        @Override
        protected RelNode visitTable(Table node, Optional<RelNode> context)
        {
            relBuilder.scan(node.getName().getParts());
            return null;
        }

        @Override
        protected RelNode visitQuery(Query node, Optional<RelNode> context)
        {
            return process(node.getQueryBody());
        }

        @Override
        protected RelNode visitQuerySpecification(QuerySpecification node, Optional<RelNode> context)
        {
            analyzeFrom(node, context);
            return relBuilder.build();
        }

        private RelNode analyzeFrom(QuerySpecification node, Optional<RelNode> context)
        {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), context);
            }

            return withImplicitFrom(node, context);
        }

        private RelNode withImplicitFrom(QuerySpecification node, Optional<RelNode> context)
        {
            // TODO
            return null;
        }
    }
}
