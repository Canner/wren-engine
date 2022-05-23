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

import com.google.common.collect.ImmutableList;
import io.cml.sql.calcite.CmlSchema;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.testng.annotations.Test;

public class TestCalciteRelConverter
{
    private final RelOptCluster relOptCluster;
    private final SqlParser sqlParser;

    private final CalciteCatalogReader reader;

    public TestCalciteRelConverter()
    {
        CalciteSchema schema = CalciteSchema.createRootSchema(true);
        schema.add("tpch.tiny", new CmlSchema());
        this.relOptCluster = createCluster();
        this.sqlParser = new SqlParser();
        this.reader = new CalciteCatalogReader(schema, ImmutableList.of("tpch.tiny"), relOptCluster.getTypeFactory(), CalciteConnectionConfigImpl.DEFAULT);
    }

    private RelOptCluster createCluster()
    {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    @Test
    public void testBasic()
    {
        String sql = "SELECT * FROM tpch.tiny.orders";
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions());
        RelNode relNode = CalciteRelConverter.convert(relOptCluster, reader, statement);
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", relNode, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));
    }
}
