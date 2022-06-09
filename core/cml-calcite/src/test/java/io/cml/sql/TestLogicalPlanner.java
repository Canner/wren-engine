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
import com.google.common.collect.ImmutableMap;
import io.cml.calcite.CmlTable;
import io.cml.metadata.Metadata;
import io.cml.sql.analyzer.Analysis;
import io.cml.tpch.TpchMetadata;
import io.cml.tpch.TpchTable;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.Frameworks;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

public class TestLogicalPlanner
{
    private final RelOptCluster relOptCluster;
    private final SqlParser sqlParser;

    private final StatementAnalyzer analyzer;

    private final CalciteCatalogReader reader;

    private final Metadata metadata;

    public TestLogicalPlanner()
    {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        SchemaPlus schema = Frameworks.createRootSchema(true);
        SchemaPlus tiny = schema.add("tiny", new AbstractSchema());

        ImmutableMap.Builder<String, CmlTable> cmlTableBuilder = ImmutableMap.builder();
        for (TpchTable table : TpchTable.values()) {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
            for (TpchTable.Column column : table.columns) {
                RelDataType type = typeFactory.createJavaType(column.type);
                builder.add(column.name, type.getSqlTypeName()).nullable(true);
            }
            tiny.add(table.name().toLowerCase(Locale.ROOT), new CmlTable(table.name().toLowerCase(Locale.ROOT), builder.build()));
            cmlTableBuilder.put(table.name().toLowerCase(Locale.ROOT), new CmlTable(table.name().toLowerCase(Locale.ROOT), builder.build()));
        }

        this.metadata = new TpchMetadata(cmlTableBuilder.build());
        this.analyzer = new StatementAnalyzer(metadata);
        this.relOptCluster = createCluster(typeFactory);
        this.sqlParser = new SqlParser();
        this.reader = new CalciteCatalogReader(CalciteSchema.from(schema), ImmutableList.of(), relOptCluster.getTypeFactory(), CalciteConnectionConfigImpl.DEFAULT);
    }

    private RelOptCluster createCluster(RelDataTypeFactory typeFactory)
    {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    @Test
    public void testBasic()
    {
        String sql = "SELECT * FROM tpch.tiny.orders";
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions());
        Analysis analysis = analyzer.analyze(new Analysis(statement), statement);
        LogicalPlanner planner = new LogicalPlanner(analysis, relOptCluster, reader, metadata);
        RelNode relNode = planner.plan(statement);
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", relNode, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));
    }

    @Test
    public void testTpch1()
            throws URISyntaxException, IOException
    {
        Path path = Paths.get(requireNonNull(getClass().getClassLoader().getResource("tpch/q1.sql")).toURI());
        String sql = Files.readString(path);
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions());
        Analysis analysis = analyzer.analyze(new Analysis(statement), statement);
        LogicalPlanner planner = new LogicalPlanner(analysis, relOptCluster, reader, metadata);
        RelNode relNode = planner.plan(statement);
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", relNode, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));
    }
}
