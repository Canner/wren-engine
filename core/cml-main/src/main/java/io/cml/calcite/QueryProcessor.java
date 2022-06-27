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
import io.airlift.log.Logger;
import io.cml.metadata.ColumnSchema;
import io.cml.metadata.ConnectorTableSchema;
import io.cml.metadata.Metadata;
import io.cml.metadata.TableHandle;
import io.cml.metadata.TableSchema;
import io.cml.spi.metadata.MaterializedViewDefinition;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.cml.metadata.MetadataUtil.createQualifiedObjectName;
import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.plan.RelOptRules.MATERIALIZATION_RULES;

public class QueryProcessor
{
    private static final Logger LOG = Logger.get(QueryProcessor.class);
    private final SqlDialect dialect;
    private final RelDataTypeFactory typeFactory;
    private final SqlParser sqlParser;
    private final RelOptCluster cluster;
    private final RelOptPlanner planner;
    private final Metadata metadata;
    private final CalciteConnectionConfig config;

    public static QueryProcessor of(Metadata metadata)
    {
        return new QueryProcessor(metadata);
    }

    private QueryProcessor(Metadata metadata)
    {
        this.dialect = requireNonNull(metadata.getDialect().getSqlDialect(), "dialect is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeFactory = new JavaTypeFactoryImpl();

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        this.config = new CalciteConnectionConfigImpl(props);

        this.cluster = newCluster(typeFactory);
        this.sqlParser = new SqlParser();
        this.planner = new HepPlanner(
                new HepProgramBuilder()
                        .addRuleCollection(MATERIALIZATION_RULES)
                        .build());
    }

    public String convert(String sql)
    {
        LOG.info("[Input query]: %s", sql);
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions());
        LOG.info("[Parsed query]: %s", SqlFormatter.formatSql(statement));
        Analysis analysis = new Analysis();
        SqlNode calciteStatement = CalciteSqlNodeConverter.convert(statement, analysis);

        List<TableSchema> visitedTable = analysis.getVisitedTables()
                .stream().map(this::toTableSchema)
                .collect(Collectors.toList());

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(CmlSchemaUtil.schemaPlus(visitedTable)),
                Collections.singletonList(""),
                typeFactory,
                config);
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

        // Validate the initial AST
        SqlNode validNode = validator.validate(calciteStatement);

        SqlToRelConverter relConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        // Convert the valid AST into a logical plan
        RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

        LOG.info(RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        // TODO: handle bigquery SQL syntax, `project.dataset.table`
        for (MaterializedViewDefinition mvDef : metadata.listMaterializedViews(Optional.empty())) {
            try {
                planner.addMaterialization(getMvRel(mvDef));
            }
            catch (Exception ex) {
                LOG.error(ex, "planner add mv failed name: %s, sql: %s", mvDef.getSchemaTableName(), mvDef.getOriginalSql());
            }
        }

        planner.setRoot(logPlan);
        // Start the optimization process to obtain the most efficient plan based on the
        // provided rule set.
        RelNode bestExp = planner.findBestExp();

        LOG.info(RelOptUtil.dumpPlan("[Optimized Logical plan]", bestExp, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
        SqlNode sqlNode = relToSqlConverter.visitRoot(bestExp).asStatement();

        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter(
                SqlWriterConfig.of().withDialect(dialect));
        return sqlPrettyWriter.format(sqlNode);
    }

    private TableSchema toTableSchema(QualifiedName tableName)
    {
        QualifiedObjectName name = createQualifiedObjectName(tableName);
        Optional<TableHandle> tableHandle = metadata.getTableHandle(name);
        return metadata.getTableSchema(tableHandle.get());
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory)
    {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private RelOptMaterialization getMvRel(MaterializedViewDefinition mvDef)
    {
        Statement statement = sqlParser.createStatement(mvDef.getOriginalSql(), new ParsingOptions());
        Analysis analysis = new Analysis();
        SqlNode calciteStatement = CalciteSqlNodeConverter.convert(statement, analysis);

        List<TableSchema> visitedTable = analysis.getVisitedTables()
                .stream().map(this::toTableSchema)
                .collect(Collectors.toList());

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(
                        CmlSchemaUtil.schemaPlus(
                                ImmutableList.<TableSchema>builder()
                                        .addAll(visitedTable)
                                        .add(toTableSchema(mvDef))
                                        .build())),
                Collections.singletonList(""),
                typeFactory,
                config);
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT);

        SqlNode validNode = validator.validate(calciteStatement);

        // TODO: find another way to get table rel instead of using calcite SqlToRelConverter
        SqlToRelConverter relConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        List<String> mvName = ImmutableList.of(
                mvDef.getCatalogName().getCatalogName(),
                mvDef.getSchemaTableName().getSchemaName(),
                mvDef.getSchemaTableName().getTableName());
        RelOptTable table = catalogReader.getTable(mvName);
        // TODO: find a way to not use relConverter convert RelOptTable to RelNode unless we are confident that doing this is the right way
        RelNode tableRel = relConverter.toRel(table, ImmutableList.of());
        RelNode queryRel = relConverter.convertQuery(validNode, false, true).rel;

        LOG.info(RelOptUtil.dumpPlan("[MV " + mvName(mvDef) + "Logical plan]", queryRel, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        return new RelOptMaterialization(
                castNonNull(tableRel),
                castNonNull(queryRel),
                null,
                mvName);
    }

    private static String mvName(MaterializedViewDefinition mvDef)
    {
        return mvDef.getCatalogName().getCatalogName() + "." +
                mvDef.getSchemaTableName().getSchemaName() + "." +
                mvDef.getSchemaTableName().getTableName();
    }

    private static TableSchema toTableSchema(MaterializedViewDefinition mvDef)
    {
        return new TableSchema(
                mvDef.getCatalogName(),
                new ConnectorTableSchema(
                        mvDef.getSchemaTableName(),
                        mvDef.getColumns().stream()
                                .map(columnMetadata ->
                                        ColumnSchema.builder()
                                                .setName(columnMetadata.getName())
                                                .setType(columnMetadata.getType())
                                                .setHidden(false)
                                                .build())
                                .collect(toImmutableList())));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;
}
