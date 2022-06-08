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

package io.cml.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cml.calcite.CmlTable;
import io.cml.tpch.TpchTable;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.testng.annotations.Test;

import java.util.Locale;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;

public class TestRelOptimizer
{
    private final RelOptCluster relOptCluster;

    private final CalciteCatalogReader reader;

    private final RelBuilder relBuilder;

    public TestRelOptimizer()
    {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        SchemaPlus schema = Frameworks.createRootSchema(true);
        ImmutableMap.Builder<String, CmlTable> cmlTableBuilder = ImmutableMap.builder();
        for (TpchTable table : TpchTable.values()) {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
            for (TpchTable.Column column : table.columns) {
                RelDataType type = typeFactory.createJavaType(column.type);
                builder.add(column.name, type.getSqlTypeName()).nullable(true);
            }
            schema.add(table.name().toLowerCase(Locale.ROOT), new CmlTable(table.name().toLowerCase(Locale.ROOT), builder.build()));
            cmlTableBuilder.put(table.name().toLowerCase(Locale.ROOT), new CmlTable(table.name().toLowerCase(Locale.ROOT), builder.build()));
        }
        this.relOptCluster = createCluster(typeFactory);
        this.reader = new CalciteCatalogReader(CalciteSchema.from(schema), ImmutableList.of(), relOptCluster.getTypeFactory(), CalciteConnectionConfigImpl.DEFAULT);
        this.relBuilder = RelFactories.LOGICAL_BUILDER.create(relOptCluster, reader);
    }

    private RelOptCluster createCluster(RelDataTypeFactory typeFactory)
    {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    @Test
    public void testRelBuilder()
    {
        relBuilder.scan(ImmutableList.of("lineitem"));
        RexNode returnFlag = relBuilder.field(1, "lineitem", "returnflag");
        RexNode linestatus = relBuilder.field(1, "lineitem", "linestatus");
        RexNode quantity = relBuilder.field(1, "lineitem", "quantity");
        RexNode extendprice = relBuilder.field(1, "lineitem", "extendedprice");
        RexNode discount = relBuilder.field(1, "lineitem", "discount");
        RexNode shipdate = relBuilder.field(1, "lineitem", "shipdate");
        RexNode tax = relBuilder.field(1, "lineitem", "tax");

        RexNode minusOneDiscount = relBuilder.call(MINUS,
                relBuilder.literal(1),
                discount);
        relBuilder.filter(relBuilder.call(LESS_THAN_OR_EQUAL,
                shipdate,
                relBuilder.getRexBuilder().makeDateLiteral(new DateString("1998-09-02"))));
        relBuilder.aggregate(relBuilder.groupKey(returnFlag, linestatus),
                relBuilder.sum(false, "sum_qty", quantity),
                relBuilder.sum(false, "sum_base_price", extendprice),
                relBuilder.sum(false, "sum_disc_price", relBuilder.call(MULTIPLY, extendprice,
                        minusOneDiscount)),
                relBuilder.sum(false, "sum_charge", relBuilder.call(MULTIPLY, extendprice,
                        relBuilder.call(MULTIPLY,
                                minusOneDiscount,
                                relBuilder.call(PLUS, relBuilder.literal(1), tax)))),
                relBuilder.avg(false, "avg_qty", quantity),
                relBuilder.avg(false, "avg_price", extendprice),
                relBuilder.avg(false, "avg_disc", discount),
                relBuilder.count(false, "count_order"));
        relBuilder.sort(returnFlag, linestatus);
        RelNode result = relBuilder.build();
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", result, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));
    }
}
