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
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.cml.metadata.ColumnSchema;
import io.cml.metadata.ConnectorTableSchema;
import io.cml.metadata.Metadata;
import io.cml.metadata.TableHandle;
import io.cml.metadata.TableSchema;
import io.cml.spi.SessionContext;
import io.cml.spi.metadata.MaterializedViewDefinition;
import io.cml.sql.QualifiedObjectName;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.cml.common.Utils.wrapException;
import static io.cml.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
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
        this.planner = new HepPlanner(
                new HepProgramBuilder()
                        .addRuleCollection(MATERIALIZATION_RULES)
                        .build());
    }

    public String convert(String sql, SessionContext sessionContext)
    {
        LOG.info("[Input query]: %s", sql);
        Statement statement = sqlParser.createStatement(sql, new ParsingOptions(AS_DECIMAL));
        LOG.info("[Parsed query]: %s", SqlFormatter.formatSql(statement));
        Analysis analysis = new Analysis();
        SqlNode calciteStatement = CalciteSqlNodeConverter.convert(statement, analysis, metadata);

        List<TableSchema> visitedTable = analysis.getVisitedTables()
                .stream().map(name -> toTableSchema(name, sessionContext))
                .collect(Collectors.toList());

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(CmlSchemaUtil.schemaPlus(visitedTable, metadata)),
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

        LOG.info(RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        // BigQuery doesn't support LATERAL Join statement. Do decorrelate to remove correlation statement.
        RelNode decorrelated = relConverter.decorrelate(validNode, logPlan);

        LOG.info(RelOptUtil.dumpPlan("[Decorrelated Logical plan]", decorrelated, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        for (MaterializedViewDefinition mvDef : metadata.listMaterializedViews(Optional.empty())) {
            try {
                planner.addMaterialization(getMvRel(mvDef, sessionContext));
            }
            catch (Exception ex) {
                LOG.error(ex, "planner add mv failed name: %s, sql: %s", mvDef.getSchemaTableName(), mvDef.getOriginalSql());
            }
        }

        planner.setRoot(decorrelated);
        // Start the optimization process to obtain the most efficient plan based on the
        // provided rule set.
        RelNode bestExp = planner.findBestExp();

        LOG.info(RelOptUtil.dumpPlan("[Optimized Logical plan]", bestExp, SqlExplainFormat.TEXT,
                SqlExplainLevel.NON_COST_ATTRIBUTES));

        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
        SqlNode sqlNode = relToSqlConverter.visitRoot(bestExp).asStatement();

        SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter(
                // BigQuery's UDF name is case sensitivity. Because we define all pg function in lowercase,
                // force all keywords is lowercase here.
                SqlWriterConfig.of().withDialect(dialect).withKeywordsLowerCase(true));
        String result = sqlPrettyWriter.format(sqlNode);
        LOG.info("[Converted calcite dialect SQL]: %s", result);
        return result;
    }

    private TableSchema toTableSchema(QualifiedName tableName, SessionContext sessionContext)
    {
        QualifiedObjectName name = createQualifiedObjectName(tableName, sessionContext.getCatalog(), sessionContext.getSchema());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(name);
        return metadata.getTableSchema(tableHandle.get());
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory)
    {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    // We use calcite sql parser in MV since bigquery use backtick as identifier quote character
    // and BigQuery will keep unquoted identifier unchanged while pg will do lowercase.
    // And these two issues could be solved by using calcite sql parser.
    private RelOptMaterialization getMvRel(MaterializedViewDefinition mvDef, SessionContext sessionContext)
    {
        org.apache.calcite.sql.parser.SqlParser calciteParser =
                org.apache.calcite.sql.parser.SqlParser.create(
                        mvDef.getOriginalSql(),
                        BigQuerySqlDialect.DEFAULT.configureParser(
                                // Calcite BigQuery dialect won't quote all identifiers unless it contains reserved character, unlike Trino,
                                // Postgresql, BigQuery will keep unquoted identifiers UNCHANGED.
                                // e.g. SELECT * FROM Abc, BigQuery will go get table Abc, Trino and Postgresql will go get table abc
                                org.apache.calcite.sql.parser.SqlParser.config().withUnquotedCasing(Casing.UNCHANGED)));

        SqlNode calciteStatement = wrapException(calciteParser::parseQuery);
        List<TableSchema> visitedTable = extractTables(calciteStatement, false)
                .stream().map(name -> toTableSchema(name, sessionContext))
                .collect(Collectors.toList());

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(
                        CmlSchemaUtil.schemaPlus(
                                ImmutableList.<TableSchema>builder()
                                        .addAll(visitedTable)
                                        .add(toTableSchema(mvDef))
                                        .build(), metadata)),
                Collections.singletonList(""),
                typeFactory,
                config);
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT);

        SqlNode validNode = validator.validate(calciteStatement);

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

    /**
     * When we use Calcite sql parser, tables in SqlNode is SqlIdentifier while SqlIdentifier could be
     * column name or something else. Here we traverse whole SqlNode to find tables in FROM and JOIN.
     *
     * @param sqlNode sqlNode that we want to traverse
     * @param fromOrJoin boolean to check input SqlNode is from or join
     * @return visited tables which is a set of QualifiedName
     */
    static Set<QualifiedName> extractTables(SqlNode sqlNode, boolean fromOrJoin)
    {
        if (sqlNode == null) {
            return ImmutableSet.of();
        }
        switch (sqlNode.getKind()) {
            case SELECT:
                var sqlSelect = (SqlSelect) sqlNode;
                var tablesInFrom = extractTables(sqlSelect.getFrom(), true);
                var tablesInSelectList =
                        sqlSelect.getSelectList().getList().stream()
                                .filter(node -> node instanceof SqlCall)
                                .map(node -> extractTables(node, false))
                                .flatMap(Collection::stream)
                                .collect(toImmutableSet());
                var tablesInWhere = extractTables(sqlSelect.getWhere(), false);
                var tablesInHaving = extractTables(sqlSelect.getHaving(), false);
                return ImmutableSet.<QualifiedName>builder()
                        .addAll(tablesInFrom)
                        .addAll(tablesInSelectList)
                        .addAll(tablesInWhere)
                        .addAll(tablesInHaving)
                        .build();
            case JOIN:
                var left = extractTables(((SqlJoin) sqlNode).getLeft(), true);
                var right = extractTables(((SqlJoin) sqlNode).getRight(), true);
                return ImmutableSet.<QualifiedName>builder()
                        .addAll(left)
                        .addAll(right)
                        .build();
            case AS:
                checkArgument(((SqlCall) sqlNode).operandCount() >= 2);
                return extractTables(((SqlCall) sqlNode).operand(0), fromOrJoin);
            case IDENTIFIER:
                if (fromOrJoin) {
                    return ImmutableSet.of(
                            QualifiedName.of(((SqlIdentifier) sqlNode).names
                                    .stream().map(Identifier::new).collect(toImmutableList())));
                }
                return ImmutableSet.of();
            default: {
                if (sqlNode instanceof SqlCall) {
                    return ((SqlCall) sqlNode).getOperandList().stream()
                            .map(sqlCall -> extractTables(sqlCall, false))
                            .flatMap(Collection::stream)
                            .collect(toImmutableSet());
                }
                return ImmutableSet.of();
            }
        }
    }
}
