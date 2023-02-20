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

package io.graphmdl.main.calcite;

import io.airlift.log.Logger;
import io.graphmdl.base.SessionContext;
import io.graphmdl.base.metadata.TableMetadata;
import io.graphmdl.main.metadata.Metadata;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
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
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.graphmdl.sqlrewrite.Utils.toCatalogSchemaTableName;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.util.Objects.requireNonNull;

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

    // TODO: abstract query processor https://github.com/Canner/canner-metric-layer/issues/68
    private QueryProcessor(Metadata metadata)
    {
        this.dialect = requireNonNull(metadata.getDialect().getSqlDialect(), "dialect is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeFactory = metadata.getTypeFactory();

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        this.config = new CalciteConnectionConfigImpl(props);

        this.cluster = newCluster(typeFactory);
        this.sqlParser = new SqlParser();
        this.planner = new HepPlanner(new HepProgramBuilder().build());
    }

    public String convert(String sql, SessionContext sessionContext)
    {
        LOG.debug("[Input SQL]: %s", sql);
        RelNode relNode = convertSqlToRelNode(sql, sessionContext);
        SqlNode sqlNode = new RelToSqlConverter(dialect).visitRoot(relNode).asStatement();

        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter(SqlWriterConfig.of().withDialect(dialect));
        String result = sqlPrettyWriter.format(sqlNode);
        LOG.debug("[Dialect SQL]: %s", result);
        return result;
    }

    private TableMetadata toTableMetadata(QualifiedName tableName, SessionContext sessionContext)
    {
        return metadata.getTableMetadata(toCatalogSchemaTableName(sessionContext, tableName));
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory)
    {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

    private RelNode convertSqlToRelNode(String sql, SessionContext sessionContext)
    {
        LOG.info("[Input query]: %s", sql);
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        LOG.debug("[Parsed query]: %s", SqlFormatter.formatSql(statement));
        Analysis analysis = new Analysis();
        SqlNode calciteStatement = CalciteSqlNodeConverter.convert(statement, analysis, metadata);

        List<TableMetadata> visitedTable = analysis.getVisitedTables()
                .stream().map(name -> toTableMetadata(name, sessionContext))
                .collect(toImmutableList());

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(GraphMDLSchemaUtil.schemaPlus(visitedTable, metadata)),
                Collections.singletonList(""),
                typeFactory,
                config);

        SqlOperatorTable chainedSqlOperatorTable = SqlOperatorTables.chain(SqlStdOperatorTable.instance(), metadata.getCalciteOperatorTable());
        SqlValidator validator = SqlValidatorUtil.newValidator(chainedSqlOperatorTable,
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT.withConformance(dialect.getConformance()));

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

        LOG.debug(RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        // BigQuery doesn't support LATERAL Join statement. Do decorrelate to remove correlation statement.
        RelNode decorrelated = relConverter.decorrelate(validNode, logPlan);

        LOG.debug(RelOptUtil.dumpPlan("[Decorrelated Logical plan]", decorrelated, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        return decorrelated;
    }
}
