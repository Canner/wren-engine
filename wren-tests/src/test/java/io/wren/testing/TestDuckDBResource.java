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

package io.wren.testing;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.wren.base.dto.Manifest;
import io.wren.main.connector.duckdb.DuckDBMetadata;
import io.wren.main.web.dto.QueryResultDto;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.wren.base.client.duckdb.DuckDBConnectorConfig.DUCKDB_CONNECTOR_INIT_SQL_PATH;
import static io.wren.base.client.duckdb.DuckDBConnectorConfig.DUCKDB_CONNECTOR_SESSION_SQL_PATH;
import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.wren.testing.WebApplicationExceptionAssert.assertWebApplicationException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@Test(singleThreaded = true)
public class TestDuckDBResource
        extends RequireWrenServer
{
    private static final String INIT_SQL_1 = "CREATE TABLE customer (custkey integer, name varchar(25), address varchar(40));";
    private static final String INIT_SQL_2 = "CREATE TABLE orders (orderkey integer, custkey integer);";
    private static final String SHOW_TABLES_SQL = "SHOW TABLES;";
    private static final String SESSION_SQL_1 = "SET s3_region = 'us-east-2';";
    private static final String SESSION_SQL_2 = "SET temp_directory = '.tmp';";
    private static final String INVALID_SQL = "xxx";

    private Path settingDir;

    @Override
    protected TestingWrenServer createWrenServer()
    {
        try {
            Path mdlDir = Files.createTempDirectory("wren-mdl");
            Path wrenMDLFilePath = mdlDir.resolve("duckdb_mdl.json");
            Manifest initial = Manifest.builder()
                    .setCatalog("memory")
                    .setSchema("tpch")
                    .build();
            Files.write(wrenMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(initial));

            settingDir = Files.createTempDirectory("duckdb-setting");

            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("wren.datasource.type", DUCKDB.name())
                    .put("wren.directory", mdlDir.toAbsolutePath().toString())
                    .put(DUCKDB_CONNECTOR_INIT_SQL_PATH, settingDir.resolve("init.sql").toAbsolutePath().toString())
                    .put(DUCKDB_CONNECTOR_SESSION_SQL_PATH, settingDir.resolve("session.sql").toAbsolutePath().toString());

            return TestingWrenServer.builder()
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
        DuckDBMetadata metadata = getInstance(Key.get(DuckDBMetadata.class));
        metadata.setInitSQL(null);
        metadata.setSessionSQL(null);
        metadata.reload();
    }

    @Test
    public void testInitSQLWithInvalidSQL()
    {
        assertWebApplicationException(() -> setDuckDBInitSQL(INVALID_SQL))
                .hasErrorMessageMatches(".*Parser Error: syntax error at or near \"xxx\"");
    }

    @Test
    public void testInitSQL()
    {
        assertThatCode(() -> setDuckDBInitSQL(INIT_SQL_1)).doesNotThrowAnyException();
        assertThat(getDuckDBInitSQL()).isEqualTo(INIT_SQL_1);
        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(QueryResultDto::getData)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(new String[] {"customer"});

        assertThatCode(() -> appendToDuckDBInitSQL(INIT_SQL_2)).doesNotThrowAnyException();
        assertThat(getDuckDBInitSQL()).isEqualTo(INIT_SQL_1 + "\n" + INIT_SQL_2);
        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(QueryResultDto::getData)
                .isNotNull()
                .asList()
                .satisfies(data -> {
                    assertThat(data).element(0).isEqualTo(new String[] {"customer"});
                    assertThat(data).element(1).isEqualTo(new String[] {"orders"});
                });

        assertThatCode(() -> setDuckDBInitSQL(INIT_SQL_1)).doesNotThrowAnyException();
        assertThat(getDuckDBInitSQL()).isEqualTo(INIT_SQL_1);
        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(QueryResultDto::getData)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(new String[] {"customer"});
    }

    @Test
    public void testAppendInitSQLBeforeSet()
    {
        assertThatCode(() -> appendToDuckDBInitSQL(INIT_SQL_1)).doesNotThrowAnyException();
        assertThat(getDuckDBInitSQL()).isEqualTo(INIT_SQL_1);
    }

    @Test
    public void testSessionSQLWithInvalidSQL()
    {
        assertWebApplicationException(() -> setDuckDBSessionSQL(INVALID_SQL))
                .hasErrorMessageMatches(".*Parser Error: syntax error at or near \"xxx\"");
    }

    @Test
    public void testAppendInvalidSessionSQLAndItWillRollback()
    {
        assertThatCode(() -> appendToDuckDBSessionSQL(SESSION_SQL_1)).doesNotThrowAnyException();
        assertWebApplicationException(() -> appendToDuckDBSessionSQL(INVALID_SQL))
                .hasErrorMessageMatches(".*Parser Error: syntax error at or near \"xxx\"");
        assertThat(getDuckDBSessionSQL()).isEqualTo(SESSION_SQL_1);
    }

    @Test
    public void testSessionSQL()
    {
        assertThatCode(() -> setDuckDBSessionSQL(SESSION_SQL_1)).doesNotThrowAnyException();
        assertThat(getDuckDBSessionSQL()).isEqualTo(SESSION_SQL_1);
        assertThat(queryDuckDB("SELECT current_setting('s3_region') AS s3_region;"))
                .extracting(QueryResultDto::getData)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(new String[] {"us-east-2"});

        assertThatCode(() -> appendToDuckDBSessionSQL(SESSION_SQL_2)).doesNotThrowAnyException();
        assertThat(getDuckDBSessionSQL()).isEqualTo(SESSION_SQL_1 + "\n" + SESSION_SQL_2);
        assertThat(queryDuckDB("SELECT current_setting('s3_region') AS s3_region, current_setting('temp_directory') AS temp_directory;"))
                .extracting(QueryResultDto::getData)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(new String[] {"us-east-2", ".tmp"});
    }

    @Test
    public void testSessionSQLEffectAllConnection()
            throws ExecutionException, InterruptedException
    {
        assertThatCode(() -> setDuckDBSessionSQL(SESSION_SQL_1)).doesNotThrowAnyException();
        assertThat(getDuckDBSessionSQL()).isEqualTo(SESSION_SQL_1);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        List<Future<QueryResultDto>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(executorService.submit(() -> queryDuckDB("SELECT current_setting('s3_region') AS s3_region;")));
        }

        for (Future<QueryResultDto> future : futures) {
            assertThat(future.get())
                    .extracting(QueryResultDto::getData)
                    .isNotNull()
                    .asList().element(0)
                    .isEqualTo(new String[] {"us-east-2"});
        }
    }

    @Test
    public void testAppendSessionSQLBeforeSet()
    {
        assertThatCode(() -> appendToDuckDBSessionSQL(SESSION_SQL_1)).doesNotThrowAnyException();
        assertThat(getDuckDBSessionSQL()).isEqualTo(SESSION_SQL_1);
    }

    @Test
    public void testQueryWithInvalidSQL()
    {
        assertWebApplicationException(() -> queryDuckDB(INVALID_SQL))
                .hasErrorMessageMatches(".*Parser Error: syntax error at or near \"xxx\"");
    }

    @Test
    public void testQuery()
    {
        assertThatCode(() -> setDuckDBInitSQL(INIT_SQL_1)).doesNotThrowAnyException();

        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(QueryResultDto::getData)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(new String[] {"customer"});
    }

    @Test(description = "We don't promote sending DDL via query API, but we don't have sql parser to validate the syntax.")
    public void testInsert()
    {
        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(QueryResultDto::getData)
                .asList().isEmpty();

        assertThatCode(() -> queryDuckDB(INIT_SQL_1)).doesNotThrowAnyException();

        assertThat(queryDuckDB(SHOW_TABLES_SQL))
                .extracting(QueryResultDto::getData)
                .isNotNull()
                .asList().element(0)
                .isEqualTo(new String[] {"customer"});
    }
}
