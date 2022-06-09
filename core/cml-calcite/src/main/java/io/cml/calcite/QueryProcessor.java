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
import io.cml.metadata.Metadata;
import io.cml.spi.connector.Connector;
import io.cml.spi.metadata.MaterializedViewDefinition;
import io.cml.sql.LogicalPlanner;
import io.cml.sql.StatementAnalyzer;
import io.cml.sql.analyzer.Analysis;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
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
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
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

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;

public class QueryProcessor
{
    private static final Logger LOG = Logger.get(QueryProcessor.class);
    private final SqlDialect dialect;
    private final Prepare.CatalogReader catalogReader;
    private final RelDataTypeFactory typeFactory;
    private final SqlParser sqlParser;
    private final RelOptCluster cluster;
    private final RelOptPlanner planner;
    private final Connector connector;
    private final Metadata metadata;

    public static QueryProcessor of(
            Metadata metadata,
            Connector connector)
    {
        return new QueryProcessor(
                metadata,
                connector);
    }

    private QueryProcessor(
            Metadata metadata,
            Connector connector)
    {
        this.dialect = requireNonNull(metadata.getDialect().getSqlDialect(), "dialect is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeFactory = new JavaTypeFactoryImpl();

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        this.catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(CmlSchemaUtil.schemaPlus(connector)),
                Collections.singletonList(""),
                typeFactory,
                config);

        this.cluster = newCluster(typeFactory);
        this.sqlParser = new SqlParser();

        this.planner = cluster.getPlanner();
        planner.addRule(CoreRules.AGGREGATE_STAR_TABLE);
        planner.addRule(MaterializedViewRules.PROJECT_JOIN);
        planner.addRule(MaterializedViewRules.PROJECT_FILTER);
        planner.addRule(MaterializedViewRules.PROJECT_AGGREGATE);
        planner.addRule(MaterializedViewRules.JOIN);
        planner.addRule(MaterializedViewRules.FILTER_SCAN);
        planner.addRule(MaterializedViewRules.AGGREGATE);
        planner.addRule(CoreRules.PROJECT_TO_CALC);
        planner.addRule(CoreRules.FILTER_TO_CALC);
        planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_UNION_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_MINUS_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_MATCH_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_WINDOW_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    }

    public String convert(String sql)
    {
        LOG.info("[Input query]: %s", sql);
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions());
        LOG.info("[Parsed query]: %s", SqlFormatter.formatSql(statement));
        StatementAnalyzer analyzer = new StatementAnalyzer(metadata);
        Analysis analysis = analyzer.analyze(new Analysis(statement), cluster, catalogReader, statement);

        LogicalPlanner toRelNode = new LogicalPlanner(analysis, cluster, catalogReader, metadata);
        RelNode relNode = toRelNode.plan(statement);

        LOG.info(RelOptUtil.dumpPlan("[Logical plan]", relNode, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        for (MaterializedViewDefinition mvDef : connector.listMaterializedViews(Optional.empty())) {
            try {
                planner.addMaterialization(getMvRel(mvDef));
            }
            catch (Exception ex) {
                LOG.error(ex, "planner add mv failed name: %s, sql: %s", mvDef.getSchemaTableName(), mvDef.getOriginalSql());
            }
        }
        // Define the type of the output plan (in this case we want a physical plan in
        // EnumerableContention)
        relNode = planner.changeTraits(relNode,
                cluster.traitSet().replace(EnumerableConvention.INSTANCE));
        planner.setRoot(relNode);
        // Start the optimization process to obtain the most efficient physical plan based on the
        // provided rule set.
        EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

        LOG.info(RelOptUtil.dumpPlan("[Optimized Logical plan]", phyPlan, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
        SqlNode sqlNode = relToSqlConverter.visitRoot(phyPlan).asStatement();

        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter(
                SqlWriterConfig.of().withDialect(dialect));
        return sqlPrettyWriter.format(sqlNode);
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory)
    {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private RelOptMaterialization getMvRel(MaterializedViewDefinition mvDef)
    {
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT);

        // TODO: find another way to get table rel instead of using calcite SqlToRelConverter
        SqlToRelConverter relConverter = new SqlToRelConverter(
                (type, query, schema, path) -> null,
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
        Statement statement = sqlParser.createStatement(mvDef.getOriginalSql(), new ParsingOptions());
        StatementAnalyzer analyzer = new StatementAnalyzer(metadata);
        Analysis analysis = analyzer.analyze(new Analysis(statement), cluster, catalogReader, statement);
        LogicalPlanner toRelNode = new LogicalPlanner(analysis, cluster, catalogReader, metadata);
        RelNode queryRel = toRelNode.plan(statement);

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
}
