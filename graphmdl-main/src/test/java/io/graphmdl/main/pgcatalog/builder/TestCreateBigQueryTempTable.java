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

package io.graphmdl.main.pgcatalog.builder;

import io.graphmdl.main.TestingMetadata;
import io.graphmdl.main.metadata.Metadata;
import org.testng.annotations.Test;

import static io.graphmdl.main.pgcatalog.builder.BigQueryUtils.createOrReplaceAllColumn;
import static io.graphmdl.main.pgcatalog.builder.BigQueryUtils.createOrReplaceAllTable;
import static io.graphmdl.main.pgcatalog.builder.BigQueryUtils.createOrReplacePgTypeMapping;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCreateBigQueryTempTable
{
    private final Metadata metadata;

    public TestCreateBigQueryTempTable()
    {
        this.metadata = new TestingMetadata();
    }

    @Test
    public void testAllColumns()
    {
        assertThat(createOrReplaceAllColumn(metadata))
                .isEqualTo("CREATE OR REPLACE VIEW `cml_temp.all_columns` AS " +
                        "SELECT col.column_name, col.ordinal_position, col.table_name, ptype.oid as typoid, ptype.typlen " +
                        "FROM `testing_schema1`.INFORMATION_SCHEMA.COLUMNS col " +
                        "LEFT JOIN `cml_temp.pg_type_mapping` mapping ON col.data_type = mapping.bq_type " +
                        "LEFT JOIN `pg_catalog.pg_type` ptype ON mapping.oid = ptype.oid " +
                        "UNION ALL SELECT col.column_name, col.ordinal_position, col.table_name, ptype.oid as typoid, ptype.typlen " +
                        "FROM `testing_schema2`.INFORMATION_SCHEMA.COLUMNS col " +
                        "LEFT JOIN `cml_temp.pg_type_mapping` mapping ON col.data_type = mapping.bq_type " +
                        "LEFT JOIN `pg_catalog.pg_type` ptype ON mapping.oid = ptype.oid;");
    }

    @Test
    public void testAllTables()
    {
        assertThat(createOrReplaceAllTable(metadata))
                .isEqualTo("CREATE OR REPLACE VIEW `cml_temp.all_tables` AS " +
                        "SELECT * FROM `testing_schema1`.INFORMATION_SCHEMA.TABLES " +
                        "UNION ALL SELECT * FROM `testing_schema2`.INFORMATION_SCHEMA.TABLES;");
    }

    @Test
    public void testPgTypeMapping()
    {
        assertThat(createOrReplacePgTypeMapping())
                .isEqualTo("CREATE OR REPLACE VIEW `cml_temp.pg_type_mapping` AS SELECT * FROM " +
                        "UNNEST([STRUCT<bq_type string, oid int64> ('BOOL', 16),('ARRAY<BOOL>', 1000),('BYTES', 17),('ARRAY<BYTES>', 1001),('FLOAT64', 701)," +
                        "('ARRAY<FLOAT64>', 1022),('INT64', 20),('ARRAY<INT64>', 1016),('STRING', 1043),('ARRAY<STRING>', 1015),('DATE', 1082),('ARRAY<DATE>', 1182)," +
                        "('NUMERIC', 1700),('ARRAY<NUMERIC>', 1231),('TIMESTAMP', 1114),('ARRAY<TIMESTAMP>', 1115)]);");
    }
}
