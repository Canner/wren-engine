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

package io.cml.testing.bigquery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.cml.testing.AbstractWireProtocolTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestResultSetMetadata
        extends AbstractWireProtocolTest
{
    private static final String TEST_CATALOG = "canner-cml";

    @Test
    public void testGetClientInfoProperties()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            assertResultSet(connection.getMetaData().getClientInfoProperties())
                    .hasColumnCount(4)
                    .hasColumn(1, "NAME", Types.VARCHAR)
                    .hasColumn(2, "MAX_LEN", Types.INTEGER)
                    .hasColumn(3, "DEFAULT_VALUE", Types.VARCHAR)
                    .hasColumn(4, "DESCRIPTION", Types.VARCHAR)
                    .hasRows((ImmutableList.of(ImmutableList.of("ApplicationName", 63, "", "The name of the application currently utilizing the connection."))));
        }
    }

    @Test
    public void testGetTypeInfo()
            throws Exception
    {
        Map<Object, List<Object>> map = ImmutableMap.<Object, List<Object>>builder()
                .put("bool", asList("bool", -7, 0, "'", "'", null, 1, false, 3, true, false, false, null, 0, 0, null, null, 10))
                .put("int2", asList("int2", 5, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("int4", asList("int4", 4, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("int8", asList("int8", -5, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("float4", asList("float4", 7, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("float8", asList("float8", 8, 0, null, null, null, 1, false, 3, false, false, false, null, 0, 0, null, null, 10))
                .put("numeric", asList("numeric", 2, 1000, null, null, null, 1, false, 3, false, false, false, null, 0, 1000, null, null, 10))
                .put("varchar", asList("varchar", 12, 10485760, "'", "'", null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10))
                .put("char", asList("char", 1, 0, "'", "'", null, 1, true, 3, true, false, false, null, 0, 0, null, null, 10))
                .put("timestamp", asList("timestamp", 93, 6, "'", "'", null, 1, false, 3, true, false, false, null, 0, 0, null, null, 10))
                .put("timestamptz", asList("timestamptz", 93, 6, "'", "'", null, 1, false, 3, true, false, false, null, 0, 0, null, null, 10))
                .build();

        try (Connection connection = createConnection()) {
            ResultSet resultSet = connection.getMetaData().getTypeInfo();
            while (resultSet.next()) {
                String type = resultSet.getString("TYPE_NAME");
                if (map.containsKey(type)) {
                    assertColumnSpec(resultSet, map.get(type));
                }
            }
        }
    }

    private static void assertColumnSpec(ResultSet rs, List<Object> expects)
            throws SQLException
    {
        String message = " of " + expects.get(0) + ": ";
        assertEquals(rs.getObject("TYPE_NAME"), expects.get(0), "TYPE_NAME" + message);
        assertEquals(rs.getObject("DATA_TYPE"), expects.get(1), "DATA_TYPE" + message);
        assertEquals(rs.getObject("PRECISION"), expects.get(2), "PRECISION" + message);
        assertEquals(rs.getObject("LITERAL_PREFIX"), expects.get(3), "LITERAL_PREFIX" + message);
        assertEquals(rs.getObject("LITERAL_SUFFIX"), expects.get(4), "LITERAL_SUFFIX" + message);
        assertEquals(rs.getObject("CREATE_PARAMS"), expects.get(5), "CREATE_PARAMS" + message);
        assertEquals(rs.getObject("NULLABLE"), expects.get(6), "NULLABLE" + message);
        assertEquals(rs.getObject("CASE_SENSITIVE"), expects.get(7), "CASE_SENSITIVE" + message);
        assertEquals(rs.getObject("SEARCHABLE"), expects.get(8), "SEARCHABLE" + message);
        assertEquals(rs.getObject("UNSIGNED_ATTRIBUTE"), expects.get(9), "UNSIGNED_ATTRIBUTE" + message);
        assertEquals(rs.getObject("FIXED_PREC_SCALE"), expects.get(10), "FIXED_PREC_SCALE" + message);
        assertEquals(rs.getObject("AUTO_INCREMENT"), expects.get(11), "AUTO_INCREMENT" + message);
        assertEquals(rs.getObject("LOCAL_TYPE_NAME"), expects.get(12), "LOCAL_TYPE_NAME" + message);
        assertEquals(rs.getObject("MINIMUM_SCALE"), expects.get(13), "MINIMUM_SCALE" + message);
        assertEquals(rs.getObject("MAXIMUM_SCALE"), expects.get(14), "MAXIMUM_SCALE" + message);
        assertEquals(rs.getObject("SQL_DATA_TYPE"), expects.get(15), "SQL_DATA_TYPE" + message);
        assertEquals(rs.getObject("SQL_DATETIME_SUB"), expects.get(16), "SQL_DATETIME_SUB" + message);
        assertEquals(rs.getObject("NUM_PREC_RADIX"), expects.get(17), "NUM_PREC_RADIX" + message);
    }

    @Test
    public void testGetUrl()
            throws Exception
    {
        HostAndPort hostAndPort = server().getPgHostAndPort();
        String url = format("jdbc:postgresql://%s:%s/%s", hostAndPort.getHost(), hostAndPort.getPort(), "test");
        try (Connection connection = this.createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getURL(), url);
        }
    }

    @Test
    public void testGetDatabaseProductVersion()
            throws Exception
    {
        try (Connection connection = this.createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getDatabaseProductName(), "PostgreSQL");
            assertEquals(metaData.getDatabaseProductVersion(), "13.0");
            assertEquals(metaData.getDatabaseMajorVersion(), 13);
            assertEquals(metaData.getDatabaseMinorVersion(), 0);
        }
    }

    @Test
    public void testGetCatalogs()
            throws Exception
    {
        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getCatalogs();
            assertThat(readRows(rs)).isEqualTo(List.of(List.of("test")));
            ResultSetMetaData metadata = rs.getMetaData();
            assertEquals(metadata.getColumnCount(), 1);
            assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
            assertEquals(metadata.getColumnType(1), Types.VARCHAR);
        }
    }

    @Test
    public void testGetSchemas()
            throws Exception
    {
        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas();
            assertGetSchemasResult(rs, List.of(List.of("cml_temp"), List.of("pg_catalog"), List.of("tpch_sf1"), List.of("tpch_tiny")));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas(null, null);
            assertGetSchemasResult(rs, List.of(List.of("cml_temp"), List.of("pg_catalog"), List.of("tpch_sf1"), List.of("tpch_tiny")));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, null);
            assertGetSchemasResult(rs, List.of(List.of("cml_temp"), List.of("pg_catalog"), List.of("tpch_sf1"), List.of("tpch_tiny")));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas("unknown", null);
            assertGetSchemasResult(rs, List.of(List.of("cml_temp"), List.of("pg_catalog"), List.of("tpch_sf1"), List.of("tpch_tiny")));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas("", null);
            assertGetSchemasResult(rs, List.of(List.of("cml_temp"), List.of("pg_catalog"), List.of("tpch_sf1"), List.of("tpch_tiny")));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, "cml_temp");
            assertGetSchemasResult(rs, List.of(List.of("cml_temp")));
        }
        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas(null, "cml_temp");
            assertGetSchemasResult(rs, List.of(List.of("cml_temp")));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas(null, "tpch%");
            assertGetSchemasResult(rs, List.of(List.of("tpch_sf1"), List.of("tpch_tiny")));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas(null, "unknown");
            assertGetSchemasResult(rs, List.of());
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas(TEST_CATALOG, "unknown");
            assertGetSchemasResult(rs, List.of());
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas("unknown", "unknown");
            assertGetSchemasResult(rs, List.of());
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getSchemas(null, "te_");
            assertGetSchemasResult(rs, List.of());
        }
    }

    private static void assertGetSchemasResult(ResultSet rs, List<List<String>> expectedSchemas)
            throws SQLException
    {
        assertThatNoException().isThrownBy(() -> readRows(rs));

        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 2);

        assertEquals(metadata.getColumnLabel(1), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(1), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_CATALOG");
        assertEquals(metadata.getColumnType(2), Types.BIGINT);
    }

    @Test
    public void testGetTables()
            throws Exception
    {
        // The first argument of getTables will be ignored by pg jdbc.
        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(null, null, null, null);
            assertThatNoException().isThrownBy(() -> readRows(rs));
        }

        // The first argument of getTables will be ignored by pg jdbc.
        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, null, null, null);
            assertThatNoException().isThrownBy(() -> readRows(rs));
        }

        // The first argument of getTables will be ignored by pg jdbc.
        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables("", null, null, null);
            assertThatNoException().isThrownBy(() -> readRows(rs));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_catalog", null, null);
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "", null, null);
            assertTableMetadata(rs);
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_catalog", "pg_class", null);
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_catalog", "pg_class", array("SYSTEM TABLE"));
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(null, "pg_catalog", null, null);
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(null, null, "pg_class", null);
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(null, null, null, array("SYSTEM TABLE"));
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_cata%", "pg_class", null);
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_cata%", "pg_class", null);
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_catalog", "pg_cla%s", null);
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables("unknown", "pg_catalog", "pg_class", array("SYSTEM TABLE"));
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "unknown", "pg_class", array("SYSTEM TABLE"));
            assertTableMetadata(rs);
            assertThat(readRows(rs).isEmpty()).isTrue();
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_catalog", "unknown", array("SYSTEM TABLE"));
            assertTableMetadata(rs);
            assertThat(readRows(rs).isEmpty()).isTrue();
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_catalog", "pg_class", array("unknown"));
            assertTableMetadata(rs);
            assertThat(readRows(rs).isEmpty()).isTrue();
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_catalog", "pg_class", array("unknown", "SYSTEM TABLE"));
            assertTableMetadata(rs);
            assertThat(readRows(rs)).contains(getSystemTablesRow("pg_catalog", "pg_class"));
        }

        try (Connection connection = this.createConnection()) {
            ResultSet rs = connection.getMetaData().getTables(TEST_CATALOG, "pg_catalog", "pg_class", array());
            assertTableMetadata(rs);
            assertThat(readRows(rs).isEmpty()).isTrue();
        }
    }

    private static List<Object> getSystemTablesRow(String schema, String table)
    {
        return getTablesRow(null, schema, table, "SYSTEM TABLE");
    }

    private static List<Object> getTablesRow(String catalog, String schema, String table, String type)
    {
        return asList(catalog, schema, table, type, null, "", "", "", "", "");
    }

    // this assertion has a little different with postgresql, the column if unquoted is lower case in postgresql, but we didn't follow this rule.
    private static void assertTableMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 10);

        assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.BIGINT);

        assertEquals(metadata.getColumnLabel(2), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(3), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(4), "TABLE_TYPE");
        assertEquals(metadata.getColumnType(4), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(5), "REMARKS");
        assertEquals(metadata.getColumnType(5), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(6), "TYPE_CAT");
        assertEquals(metadata.getColumnType(6), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(7), "TYPE_SCHEM");
        assertEquals(metadata.getColumnType(7), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(8), "TYPE_NAME");
        assertEquals(metadata.getColumnType(8), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(9), "SELF_REFERENCING_COL_NAME");
        assertEquals(metadata.getColumnType(9), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(10), "REF_GENERATION");
        assertEquals(metadata.getColumnType(10), Types.VARCHAR);
    }

    @Test
    public void testGetTableTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getTableTypes();
            assertThat(readRows(rs)).isEqualTo(List.of(
                    List.of("FOREIGN TABLE"),
                    List.of("INDEX"),
                    List.of("MATERIALIZED VIEW"),
                    List.of("PARTITIONED INDEX"),
                    List.of("PARTITIONED TABLE"),
                    List.of("SEQUENCE"),
                    List.of("SYSTEM INDEX"),
                    List.of("SYSTEM TABLE"),
                    List.of("SYSTEM TOAST INDEX"),
                    List.of("SYSTEM TOAST TABLE"),
                    List.of("SYSTEM VIEW"),
                    List.of("TABLE"),
                    List.of("TEMPORARY INDEX"),
                    List.of("TEMPORARY SEQUENCE"),
                    List.of("TEMPORARY TABLE"),
                    List.of("TEMPORARY VIEW"),
                    List.of("TYPE"),
                    List.of("VIEW")));

            ResultSetMetaData metadata = rs.getMetaData();
            assertEquals(metadata.getColumnCount(), 1);

            assertEquals(metadata.getColumnLabel(1), "TABLE_TYPE");
            assertEquals(metadata.getColumnType(1), Types.VARCHAR);
        }
    }

    @Test
    public void testGetColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getColumns(null, null, "pg_class", "relnamespace");
            assertColumnMetadata(rs);
            assertThat(readRows(rs)).hasSize(1);
        }

        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, null, "pg_class", "relnamespace");
            assertColumnMetadata(rs);
            assertThat(readRows(rs)).hasSize(1);
        }

        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getColumns(null, "pg_catalog", "pg_class", "relnamespace");
            assertColumnMetadata(rs);
            assertThat(readRows(rs)).hasSize(1);
        }

        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "pg_catalog", "pg_class", "relnamespace");
            assertColumnMetadata(rs);
            assertThat(readRows(rs)).hasSize(1);
        }

        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "pg_catal%", "pg_class", "relnamespace");
            assertColumnMetadata(rs);
            assertThat(readRows(rs)).hasSize(1);
        }

        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "pg_catalog", "pg_cla%", "relnamespace");
            assertColumnMetadata(rs);
            assertThat(readRows(rs)).hasSize(1);
        }

        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getColumns(TEST_CATALOG, "pg_catalog", "pg_class", "%lnamespac%");
            assertColumnMetadata(rs);
            assertThat(readRows(rs)).hasSize(1);
        }
    }

    private static void assertColumnMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 24);

        assertEquals(metadata.getColumnLabel(1), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(2), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(3), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(4), "COLUMN_NAME");
        assertEquals(metadata.getColumnType(4), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(5), "DATA_TYPE");
        assertEquals(metadata.getColumnType(5), Types.SMALLINT);

        assertEquals(metadata.getColumnLabel(6), "TYPE_NAME");
        assertEquals(metadata.getColumnType(6), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(7), "COLUMN_SIZE");
        assertEquals(metadata.getColumnType(7), Types.INTEGER);

        assertEquals(metadata.getColumnLabel(8), "BUFFER_LENGTH");
        assertEquals(metadata.getColumnType(8), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(9), "DECIMAL_DIGITS");
        assertEquals(metadata.getColumnType(9), Types.INTEGER);

        assertEquals(metadata.getColumnLabel(10), "NUM_PREC_RADIX");
        assertEquals(metadata.getColumnType(10), Types.INTEGER);

        assertEquals(metadata.getColumnLabel(11), "NULLABLE");
        assertEquals(metadata.getColumnType(11), Types.INTEGER);

        assertEquals(metadata.getColumnLabel(12), "REMARKS");
        assertEquals(metadata.getColumnType(12), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(13), "COLUMN_DEF");
        assertEquals(metadata.getColumnType(13), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(14), "SQL_DATA_TYPE");
        assertEquals(metadata.getColumnType(14), Types.INTEGER);

        assertEquals(metadata.getColumnLabel(15), "SQL_DATETIME_SUB");
        assertEquals(metadata.getColumnType(15), Types.INTEGER);

        assertEquals(metadata.getColumnLabel(16), "CHAR_OCTET_LENGTH");
        assertEquals(metadata.getColumnType(16), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(17), "ORDINAL_POSITION");
        assertEquals(metadata.getColumnType(17), Types.INTEGER);

        assertEquals(metadata.getColumnLabel(18), "IS_NULLABLE");
        assertEquals(metadata.getColumnType(18), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(19), "SCOPE_CATALOG");
        assertEquals(metadata.getColumnType(19), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(20), "SCOPE_SCHEMA");
        assertEquals(metadata.getColumnType(20), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(21), "SCOPE_TABLE");
        assertEquals(metadata.getColumnType(21), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(22), "SOURCE_DATA_TYPE");
        assertEquals(metadata.getColumnType(22), Types.SMALLINT);

        assertEquals(metadata.getColumnLabel(23), "IS_AUTOINCREMENT");
        assertEquals(metadata.getColumnType(23), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(24), "IS_GENERATEDCOLUMN");
        assertEquals(metadata.getColumnType(24), Types.VARCHAR);
    }

    @Test
    public void testGetPseudoColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            assertThatThrownBy(() -> connection.getMetaData().getPseudoColumns(null, null, null, null))
                    .isInstanceOf(SQLException.class)
                    .hasMessage("Method org.postgresql.jdbc.PgDatabaseMetaData.getPseudoColumns(String, String, String, String) is not yet implemented.");
        }
    }

    @Test
    public void testGetFunctions()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getFunctions(null, null, "current_schemas");
            assertTrue(rs.next());
            assertEquals(rs.getString("function_cat"), "canner-cml");
            assertEquals(rs.getString("function_schem"), "pg_catalog");
            assertEquals(rs.getString("function_name"), "current_schemas");
            assertNull(rs.getString("remarks"));
            assertEquals(rs.getInt("function_type"), 1);
            assertEquals(rs.getString("specific_name"), "current_schemas_8097498532701726860");
        }
    }

    @Test
    public void testGetProcedures()
            throws Exception
    {
        // Pgsql driver method getProcedures can not get function like "current_schemas"
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getProcedures(null, null, "current_schemas");
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetProcedureColumns()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getProcedureColumns(null, null, null, null);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetSuperTables()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            assertThatThrownBy(() -> connection.getMetaData().getSuperTables(null, null, null))
                    .isInstanceOf(SQLException.class)
                    .hasMessage("Method org.postgresql.jdbc.PgDatabaseMetaData.getSuperTables(String,String,String,String) is not yet implemented.");
        }
    }

    @Test(enabled = false, description = "Pgsql driver 42.3.1 not supported")
    public void testGetUdts()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.getMetaData().getUDTs(null, null, null, null);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGetAttributes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            assertThatThrownBy(() -> connection.getMetaData().getAttributes(null, null, null, null))
                    .isInstanceOf(SQLException.class)
                    .hasMessage("Method org.postgresql.jdbc.PgDatabaseMetaData.getAttributes(String,String,String,String) is not yet implemented.");
        }
    }

    @Test
    public void testGetSuperTypes()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            assertThatThrownBy(() -> connection.getMetaData().getSuperTypes(null, null, null))
                    .isInstanceOf(SQLException.class)
                    .hasMessage("Method org.postgresql.jdbc.PgDatabaseMetaData.getSuperTypes(String,String,String) is not yet implemented.");
        }
    }

    public static ResultSetAssert assertResultSet(ResultSet resultSet)
    {
        return new ResultSetAssert(resultSet);
    }

    public static class ResultSetAssert
    {
        private final ResultSet resultSet;

        public ResultSetAssert(ResultSet resultSet)
        {
            this.resultSet = requireNonNull(resultSet, "resultSet is null");
        }

        public ResultSetAssert hasColumnCount(int expectedColumnCount)
                throws SQLException
        {
            assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(expectedColumnCount);
            return this;
        }

        public ResultSetAssert hasColumn(int columnIndex, String name, int sqlType)
                throws SQLException
        {
            assertThat(resultSet.getMetaData().getColumnName(columnIndex)).isEqualTo(name);
            assertThat(resultSet.getMetaData().getColumnType(columnIndex)).isEqualTo(sqlType);
            return this;
        }

        public ResultSetAssert hasRows(List<List<?>> expected)
                throws SQLException
        {
            assertThat(readRows(resultSet)).isEqualTo(expected);
            return this;
        }
    }

    public static List<List<Object>> readRows(ResultSet rs)
            throws SQLException
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }
        return rows.build();
    }

    @SafeVarargs
    public static <T> List<T> list(T... elements)
    {
        return asList(elements);
    }

    @SafeVarargs
    public static <T> T[] array(T... elements)
    {
        return elements;
    }
}
