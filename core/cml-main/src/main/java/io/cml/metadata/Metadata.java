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
package io.cml.metadata;

import io.cml.calcite.CmlSchemaUtil;
import io.cml.spi.Column;
import io.cml.spi.ConnectorRecordIterator;
import io.cml.spi.Parameter;
import io.cml.spi.SessionContext;
import io.cml.spi.metadata.MaterializedViewDefinition;
import io.cml.spi.metadata.SchemaTableName;
import io.cml.spi.metadata.TableMetadata;
import io.cml.sql.QualifiedObjectName;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlOperatorTable;

import java.util.List;
import java.util.Optional;

public interface Metadata
{
    void createSchema(String name);

    boolean isSchemaExist(String name);

    List<String> listSchemas();

    List<TableMetadata> listTables(String schemaName);

    /**
     * Create mv by using pg syntax sql, each connector should
     * convert pg syntax sql to connector-compatible sql by using
     * {@link io.cml.calcite.QueryProcessor#convert(String, SessionContext)}
     * before create mv.
     * <p>
     * All mvs in canner-metric-layer should be placed under {@link Metadata#getMaterializedViewSchema}
     * and use TABLE to create mv instead of using MATERIALIZED VIEW since in bq/snowflake
     * there are many limitation in mv.
     * <p>
     * More info, see:
     * <li><a href=https://cloud.google.com/bigquery/docs/materialized-views-intro#limitations>bigquery mv limitations</a></li>
     * <li><a href=https://docs.snowflake.com/en/user-guide/views-materialized.html#limitations-on-creating-materialized-views>snowflake mv limitations</a></li>
     *
     * @param schemaTableName the table name we want to create
     * @param sql mv sql that follow pg syntax
     */
    void createMaterializedView(SchemaTableName schemaTableName, String sql);

    /**
     * List all MVs, note that mv here is not the same as bigquery/snowflake mv,
     * mv here is so-called "metric" which is backed by table, and all these mvs will
     * be placed under {@link #getMaterializedViewSchema()}.
     *
     * @return list of MaterializedViewDefinition
     */
    List<MaterializedViewDefinition> listMaterializedViews();

    /**
     * Delete the specific materializedView.
     * <p>
     * Because we use TABLE to present the concept of materializedView in BigQuery,
     * this method drop the specific TABLE in {@link Metadata#getMaterializedViewSchema} actually.
     *
     * @param schemaTableName the specific table name
     */
    void deleteMaterializedView(SchemaTableName schemaTableName);

    List<String> listFunctionNames(String schemaName);

    String resolveFunction(String functionName, int numArgument);

    SqlOperatorTable getCalciteOperatorTable();

    RelDataTypeFactory getTypeFactory();

    TableSchema getTableSchema(TableHandle tableHandle);

    Optional<TableHandle> getTableHandle(QualifiedObjectName tableName);

    CmlSchemaUtil.Dialect getDialect();

    RelDataTypeSystem getRelDataTypeSystem();

    String getDefaultCatalog();

    void directDDL(String sql);

    ConnectorRecordIterator directQuery(String sql, List<Parameter> parameters);

    List<Column> describeQuery(String sql, List<Parameter> parameters);

    default String getMaterializedViewSchema()
    {
        return "cml_mvs";
    }
}
