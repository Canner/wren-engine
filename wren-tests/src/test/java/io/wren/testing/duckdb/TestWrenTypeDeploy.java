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

package io.wren.testing.duckdb;

import io.wren.base.dto.Manifest;
import io.wren.base.dto.Model;
import io.wren.main.web.dto.DeployInputDto;
import org.testng.annotations.Test;

import java.util.List;

import static io.wren.base.dto.Column.column;
import static io.wren.testing.WebApplicationExceptionAssert.assertWebApplicationException;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestWrenTypeDeploy
        extends AbstractWireProtocolTestWithDuckDB
{
    private final Manifest stdManifest = Manifest.builder()
            .setCatalog("test")
            .setSchema("duck")
            .setModels(List.of(Model.model("type_test", "select 1",
                    List.of(column("c_bool", "BOOLEAN", null, true, "true"),
                            column("c_tinyint", "TINYINT", null, true, "1"),
                            column("c_int2", "INT2", null, true, "1"),
                            column("c_smallint", "SMALLINT", null, true, "1"),
                            column("c_int4", "INT4", null, true, "1"),
                            column("c_integer", "INTEGER", null, true, "1"),
                            column("c_int8", "INT8", null, true, "1"),
                            column("c_bigint", "BIGINT", null, true, "1"),
                            column("c_float4", "FLOAT4", null, true, "1.0"),
                            column("c_real", "REAL", null, true, "1.0"),
                            column("c_float8", "FLOAT8", null, true, "1.0"),
                            column("c_double", "DOUBLE", null, true, "1.0"),
                            column("c_numeric", "NUMERIC", null, true, "1.0"),
                            column("c_decimal", "DECIMAL", null, true, "1.0"),
                            column("c_varchar", "VARCHAR", null, true, "'1'"),
                            column("c_string", "STRING", null, true, "'1'"),
                            column("c_char", "CHAR", null, true, "'1'"),
                            column("c_json", "JSON", null, true, "'{\"key\": \"value\"}'"),
                            column("c_timestamp", "TIMESTAMP", null, true, "'2021-01-01 00:00:00'"),
                            column("c_timestamptz", "TIMESTAMP WITH TIME ZONE", null, true, "'2021-01-01 00:00:00'"),
                            column("c_text", "TEXT", null, true, "'1'"),
                            column("c_name", "NAME", null, true, "'1'"),
                            column("c_oid", "OID", null, true, "1"),
                            column("c_date", "DATE", null, true, "'2021-01-01'"),
                            column("c_bytea", "BYTEA", null, true, "'\\x01'"),
                            column("c_uuid", "UUID", null, true, "'123e4567-e89b-12d3-a456-426614174000'"),
                            column("c_interval", "INTERVAL", null, true, "interval '1' day"),
                            column("c_bpchar", "BPCHAR", null, true, "'1'"),
                            column("c_inet", "INET", null, true, "'192.168/24'"),
                            column("c_varchar_array", "VARCHAR[]", null, true, "ARRAY['1']")))))
            .build();

    @Test
    public void testDeploy()
    {
        assertThatNoException().isThrownBy(() -> deployMDL(new DeployInputDto(stdManifest, null)));

        Manifest unknownPgType = Manifest.builder()
                .setCatalog("test")
                .setSchema("duck")
                .setModels(List.of(Model.model("type_test", "select 1",
                        List.of(column("c_bool", "BOOLEAN", null, true, "true"),
                                column("c_notofund_array", "_NOTFOUND", null, true, "ARRAY['1']"),
                                column("c_notfound", "NOTFOUND", null, true, "'1'")))))
                .build();
        // We map the unknown type to varchar always
        assertThatNoException().isThrownBy(() -> deployMDL(new DeployInputDto(unknownPgType, null)));

        // Known pg type but not supported by duckdb
        Manifest invalid = Manifest.builder()
                .setCatalog("test")
                .setSchema("duck")
                .setModels(List.of(Model.model("type_test", "select 1",
                        List.of(column("c_bool", "BOOLEAN", null, true, "true"),
                                column("c_hstore", "HSTORE", null, true, "'key=>value'")))))
                .build();

        assertWebApplicationException(() -> deployMDL(new DeployInputDto(invalid, null)))
                .hasHTTPStatus(400)
                .hasErrorMessageMatches("Failed to sync PG Metastore: DuckDB unsupported Type: io.wren.base.type.HstoreType(.|\\n)*");

        Manifest invalidArray = Manifest.builder()
                .setCatalog("test")
                .setSchema("duck")
                .setModels(List.of(Model.model("type_test", "select 1",
                        List.of(column("c_bool", "BOOLEAN", null, true, "true"),
                                column("c_hstore_array", "_HSTORE", null, true, "'key=>value'")))))
                .build();

        assertWebApplicationException(() -> deployMDL(new DeployInputDto(invalidArray, null)))
                .hasHTTPStatus(400)
                .hasErrorMessageMatches("Failed to sync PG Metastore: DuckDB unsupported Type: io.wren.base.type.HstoreType(.|\\n)*");
    }
}
