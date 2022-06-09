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
import io.cml.metadata.Metadata;
import io.cml.spi.metadata.ColumnMetadata;
import io.cml.spi.metadata.TableMetadata;
import io.cml.spi.type.PGType;
import io.cml.sql.LogicalPlanner;
import io.cml.sql.StatementAnalyzer;
import io.cml.sql.analyzer.Analysis;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.Frameworks;

import java.util.List;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.cml.spi.type.BigIntType.BIGINT;
import static io.cml.spi.type.BooleanType.BOOLEAN;
import static io.cml.spi.type.DoubleType.DOUBLE;
import static io.cml.spi.type.IntegerType.INTEGER;
import static io.cml.spi.type.VarcharType.VARCHAR;

public final class CmlSchemaUtil
{
    private CmlSchemaUtil() {}

    public enum Dialect
    {
        BIGQUERY(BigQuerySqlDialect.DEFAULT);

        private final SqlDialect sqlDialect;

        Dialect(SqlDialect sqlDialect)
        {
            this.sqlDialect = sqlDialect;
        }
    }

    public static String convertQuery(Dialect dialect, SchemaPlusInfo schemaPlusInfo, Metadata metadata, String sql)
    {
        SqlParser sqlParser = new SqlParser();
        Statement stmt = sqlParser.createStatement(sql, new ParsingOptions());
        RelOptCluster cluster = newCluster();

        Analysis analysis = new Analysis(stmt);
        StatementAnalyzer statementAnalyzer = new StatementAnalyzer(metadata);

        SchemaPlus schemaPlus = get(schemaPlusInfo);
        CalciteCatalogReader reader = new CalciteCatalogReader(
                CalciteSchema.from(schemaPlus),
                ImmutableList.of(),
                cluster.getTypeFactory(),
                CalciteConnectionConfigImpl.DEFAULT);

        analysis = statementAnalyzer.analyze(analysis, stmt);
        LogicalPlanner logicalPlanner = new LogicalPlanner(analysis, cluster, reader, metadata);
        RelNode logicalRoot = logicalPlanner.plan(stmt);
        // TODO: add optimizer
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect.sqlDialect);
        SqlNode sqlNode = relToSqlConverter.visitRoot(logicalRoot).asStatement();

        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter();
        return sqlPrettyWriter.format(sqlNode);
    }

    private static RelOptCluster newCluster()
    {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    private static SchemaPlus get(SchemaPlusInfo schemaPlusInfo)
    {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        schemaPlusInfo.getSchemaTableMap()
                .forEach((schema, tables) -> rootSchema.add(schema, toCmlSchema(tables)));

        return rootSchema;
    }

    private static CmlTable toCmlTable(TableMetadata tableMetadata)
    {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            builder.add(columnMetadata.getName(), toRelDataType(typeFactory, columnMetadata.getType()));
        }

        return new CmlTable(tableMetadata.getTable().getTableName(), builder.build());
    }

    private static CmlSchema toCmlSchema(List<TableMetadata> tables)
    {
        return new CmlSchema(tables.stream().collect(

                toImmutableMap(
                        table -> table.getTable().getTableName(),
                        CmlSchemaUtil::toCmlTable,
                        // TODO: handle case sensitive table name
                        (a, b) -> a)));
    }

    // TODO: handle nested types
    private static RelDataType toRelDataType(JavaTypeFactory typeFactory, PGType<?> pgType)
    {
        if (pgType.equals(BOOLEAN)) {
            return typeFactory.createJavaType(Boolean.class);
        }
        if (pgType.equals(INTEGER)) {
            return typeFactory.createJavaType(Integer.class);
        }
        if (pgType.equals(BIGINT)) {
            return typeFactory.createJavaType(Long.class);
        }
        if (pgType.equals(VARCHAR)) {
            return typeFactory.createJavaType(String.class);
        }
        if (pgType.equals(DOUBLE)) {
            return typeFactory.createJavaType(Double.class);
        }
        throw new UnsupportedOperationException(pgType.type() + " not supported yet");
    }
}
