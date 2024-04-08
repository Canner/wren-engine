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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.wren.base.dto.Column;
import io.wren.base.dto.Manifest;
import io.wren.base.dto.Model;
import io.wren.main.web.dto.ColumnPredicateDto;
import io.wren.main.web.dto.PredicateDto;
import io.wren.main.web.dto.SqlAnalysisInputDto;
import io.wren.main.web.dto.SqlAnalysisOutputDto;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.wren.base.WrenTypes.DATE;
import static io.wren.base.WrenTypes.INTEGER;
import static io.wren.base.WrenTypes.VARCHAR;
import static io.wren.base.dto.Column.column;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.wren.base.dto.Model.model;
import static io.wren.testing.AbstractTestFramework.withDefaultCatalogSchema;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAnalysisResource
        extends RequireWrenServer
{
    private Model customer;

    @Override
    protected TestingWrenServer createWrenServer()
    {
        initData();
        Manifest manifest = withDefaultCatalogSchema()
                .setModels(List.of(customer))
                .build();

        Path mdlDir;
        try {
            mdlDir = Files.createTempDirectory("wrenmdls");
            Path wrenMDLFilePath = mdlDir.resolve("wrenmdl.json");
            Files.write(wrenMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(manifest));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("wren.directory", mdlDir.toAbsolutePath().toString())
                .put("wren.datasource.type", "duckdb");

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    private void initData()
    {
        customer = model("Customer",
                "select * from main.customer",
                List.of(
                        column("custkey", INTEGER, null, true),
                        column("name", VARCHAR, null, true),
                        column("address", VARCHAR, null, true),
                        column("nationkey", INTEGER, null, true),
                        column("phone", VARCHAR, null, true),
                        column("acctbal", INTEGER, null, true),
                        column("mktsegment", VARCHAR, null, true),
                        column("comment", VARCHAR, null, true)),
                "custkey");
    }

    @Test
    public void testAnalysisSqlDefaultManifest()
    {
        List<SqlAnalysisOutputDto> results = getSqlAnalysis(new SqlAnalysisInputDto(null, "SELECT * FROM Customer WHERE custkey >= 100 AND custkey <= 123 OR name != 'foo'"));
        assertThat(results.size()).isEqualTo(1);

        List<SqlAnalysisOutputDto> expected = ImmutableList.<SqlAnalysisOutputDto>builder()
                .add(new SqlAnalysisOutputDto("Customer",
                        List.of(new ColumnPredicateDto("custkey", List.of(new PredicateDto(">=", "100"), new PredicateDto("<=", "123"))),
                                new ColumnPredicateDto("name", List.of(new PredicateDto("<>", "'foo'")))),
                        null,
                        null))
                .build();

        assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testAnalysisSqlCustomManifest()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(ImmutableList.of(
                        model("table_1", "SELECT * FROM foo", ImmutableList.of(varcharColumn("c1"), column("c2", INTEGER, null, true))),
                        model("table_2", "SELECT * FROM bar", ImmutableList.of(varcharColumn("c1"), column("c2", DATE, null, true)))))
                .build();

        List<SqlAnalysisOutputDto> results = getSqlAnalysis(
                new SqlAnalysisInputDto(manifest,
                        "SELECT t1.c1, t2.c1, t2.c2 FROM table_1 t1 JOIN table_2 t2 ON t1.c2 = t2.c1\n" +
                                "WHERE t1.c1 = 'foo' AND t1.c2 >= 123 OR t2.c1 != 'bar' OR t2.c2 < DATE '2020-01-01'"));
        assertThat(results.size()).isEqualTo(2);

        List<SqlAnalysisOutputDto> expected = ImmutableList.<SqlAnalysisOutputDto>builder()
                .add(new SqlAnalysisOutputDto("table_1",
                        List.of(new ColumnPredicateDto("c1", List.of(new PredicateDto("=", "'foo'"))),
                                new ColumnPredicateDto("c2", List.of(new PredicateDto(">=", "123")))),
                        null,
                        null))
                .add(new SqlAnalysisOutputDto("table_2",
                        List.of(new ColumnPredicateDto("c1", List.of(new PredicateDto("<>", "'bar'"))),
                                new ColumnPredicateDto("c2", List.of(new PredicateDto("<", "DATE '2020-01-01'")))),
                        null,
                        null))
                .build();

        assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testAnalyzesSqlWithLimitAndOrdering()
    {
        Manifest manifest = Manifest.builder()
                .setCatalog("test")
                .setSchema("test")
                .setModels(ImmutableList.of(
                        model("table_1", "SELECT * FROM foo", ImmutableList.of(varcharColumn("c1"), column("c2", INTEGER, null, true))),
                        model("table_2", "SELECT * FROM bar", ImmutableList.of(varcharColumn("c1"), column("c2", DATE, null, true)))))
                .build();

        List<SqlAnalysisOutputDto> results = getSqlAnalysis(new SqlAnalysisInputDto(manifest, "SELECT * FROM table_1 WHERE c1 > 10 ORDER BY c1 LIMIT 10"));
        assertThat(results.size()).isEqualTo(1);
        List<SqlAnalysisOutputDto> expected = ImmutableList.<SqlAnalysisOutputDto>builder()
                .add(new SqlAnalysisOutputDto("table_1",
                        List.of(new ColumnPredicateDto("c1", List.of(new PredicateDto(">", "10")))),
                        "10",
                        List.of(new SqlAnalysisOutputDto.SortItem("c1", "ASCENDING"))))
                .build();
        assertThat(results).containsExactlyInAnyOrderElementsOf(expected);

        results = getSqlAnalysis(new SqlAnalysisInputDto(manifest, "SELECT * FROM table_1 WHERE c1 > 10 ORDER BY c1 DESC LIMIT 10"));
        assertThat(results.size()).isEqualTo(1);
        expected = ImmutableList.<SqlAnalysisOutputDto>builder()
                .add(new SqlAnalysisOutputDto("table_1",
                        List.of(new ColumnPredicateDto("c1", List.of(new PredicateDto(">", "10")))),
                        "10",
                        List.of(new SqlAnalysisOutputDto.SortItem("c1", "DESCENDING"))))
                .build();
        assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testSqlAnalysisEmpty()
    {
        assertThat(getSqlAnalysis(new SqlAnalysisInputDto(null, "SELECT * FROM Customer")).size()).isEqualTo(0);
        assertThat(getSqlAnalysis(new SqlAnalysisInputDto(null, "SELECT custkey = 123 FROM Customer")).size()).isEqualTo(0);
    }

    private static Column varcharColumn(String name)
    {
        return column(name, "VARCHAR", null, false, null);
    }
}
