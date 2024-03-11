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

package io.accio.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.accio.base.dto.Manifest;
import io.accio.main.web.dto.DuckDBQueryDto;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.accio.base.config.AccioConfig.DataSourceType.DUCKDB;
import static io.accio.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.accio.testing.WebApplicationExceptionAssert.assertWebApplicationException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@Test(singleThreaded = true)
public class TestDuckDBResource
        extends RequireAccioServer
{
    private static final String INIT_SQL = "CREATE TABLE customer (custkey integer, name varchar(25), address varchar(40));";
    private static final String APPEND_INIT_SQL = "CREATE TABLE orders (orderkey integer, custkey integer);";
    private static final String SHOW_TABLES_SQL = "SHOW TABLES;";
    private static final String SESSION_SQL = "SET s3_region = 'us-east-2';";
    private static final String APPEND_SESSION_SQL = "SET temp_directory = '/tmp';";
    private static final String FAILED_SQL = "xxx";

    private Path settingDir;

    @Override
    protected TestingAccioServer createAccioServer()
    {
        try {
            Path mdlDir = Files.createTempDirectory("accio-mdl");
            Path accioMDLFilePath = mdlDir.resolve("duckdb_mdl.json");
            Manifest initial = Manifest.builder()
                    .setCatalog("memory")
                    .setSchema("tpch")
                    .build();
            Files.write(accioMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(initial));

            settingDir = Files.createTempDirectory("duckdb-setting");

            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("accio.datasource.type", DUCKDB.name())
                    .put("accio.directory", mdlDir.toAbsolutePath().toString())
                    .put("duckdb.connector.setting-dir", settingDir.toAbsolutePath().toString());

            return TestingAccioServer.builder()
                    .setRequiredConfigs(properties.build())
                    .build();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @AfterMethod
    public void cleanTempDir()
            throws IOException
    {
        Files.walk(settingDir)
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    public void testInitSQLWithFailedSQL()
    {
        assertWebApplicationException(() -> setDuckDBInitSQL(FAILED_SQL))
                .hasErrorMessageMatches(".*Parser Error: syntax error at or near \"xxx\"");
    }

    @Test
    public void testInitSQL()
    {
        assertThatCode(() -> setDuckDBInitSQL(INIT_SQL)).doesNotThrowAnyException();
        assertThat(getDuckDBInitSQL()).isEqualTo(INIT_SQL);
        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(DuckDBQueryDto::getRows)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(ImmutableList.of("customer"));

        assertThatCode(() -> appendToDuckDBInitSQL(APPEND_INIT_SQL)).doesNotThrowAnyException();
        assertThat(getDuckDBInitSQL()).isEqualTo(INIT_SQL + "\n" + APPEND_INIT_SQL);
        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(DuckDBQueryDto::getRows)
                .isNotNull()
                .asList()
                .isEqualTo(ImmutableList.of(ImmutableList.of("customer"), ImmutableList.of("orders")));

        assertThatCode(() -> setDuckDBInitSQL(INIT_SQL)).doesNotThrowAnyException();
        assertThat(getDuckDBInitSQL()).isEqualTo(INIT_SQL);
        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(DuckDBQueryDto::getRows)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(ImmutableList.of("customer"));
    }

    @Test
    public void testSessionSQLWithFailedSQL()
    {
        assertWebApplicationException(() -> setDuckDBSessionSQL(FAILED_SQL))
                .hasErrorMessageMatches(".*Parser Error: syntax error at or near \"xxx\"");
    }

    @Test
    public void testSessionSQL()
    {
        assertThatCode(() -> setDuckDBSessionSQL(SESSION_SQL)).doesNotThrowAnyException();
        assertThat(getDuckDBSessionSQL()).isEqualTo(SESSION_SQL);
        assertThat(queryDuckDB("SELECT current_setting('s3_region') AS s3_region;"))
                .extracting(DuckDBQueryDto::getRows)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(ImmutableList.of("us-east-2"));

        assertThatCode(() -> appendToDuckDBSessionSQL(APPEND_SESSION_SQL)).doesNotThrowAnyException();
        assertThat(getDuckDBSessionSQL()).isEqualTo(SESSION_SQL + "\n" + APPEND_SESSION_SQL);
        assertThat(queryDuckDB("SELECT current_setting('s3_region') AS s3_region, current_setting('temp_directory') AS temp_directory;"))
                .extracting(DuckDBQueryDto::getRows)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(ImmutableList.of("us-east-2", "/tmp"));
    }

    @Test
    public void testQueryWithFailedSQL()
    {
        assertWebApplicationException(() -> queryDuckDB(FAILED_SQL))
                .hasErrorMessageMatches(".*Parser Error: syntax error at or near \"xxx\"");
    }

    @Test
    public void testQuery()
    {
        assertThatCode(() -> setDuckDBInitSQL(INIT_SQL)).doesNotThrowAnyException();

        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(DuckDBQueryDto::getRows)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(ImmutableList.of("customer"));
    }
}
