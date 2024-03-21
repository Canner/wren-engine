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
import io.wren.base.dto.Manifest;
import io.wren.base.type.BigIntType;
import io.wren.main.web.dto.CheckOutputDto;
import io.wren.main.web.dto.DeployInputDto;
import io.wren.main.web.dto.PreviewDto;
import io.wren.main.web.dto.QueryResultDto;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.wren.base.Utils.randomIntString;
import static io.wren.base.client.duckdb.FileUtil.ARCHIVED;
import static io.wren.base.dto.Column.column;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.wren.base.dto.Model.model;
import static io.wren.testing.WebApplicationExceptionAssert.assertWebApplicationException;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestMDLResource
        extends RequireWrenServer
{
    private Path wrenMDLFilePath;
    private Path mdlDir;
    private Manifest initial = Manifest.builder()
            .setCatalog("canner-cml")
            .setSchema("tpch_tiny")
            .setModels(List.of(
                    model("Orders", "SELECT * FROM \"canner-cml\".tpch_tiny.orders", List.of(column("orderkey", "integer", null, false, "o_orderkey")))))
            .build();

    private Manifest updated = Manifest.builder()
            .setCatalog("canner-cml")
            .setSchema("tpch_tiny")
            .setModels(List.of(
                    model("Orders", "SELECT * FROM \"canner-cml\".tpch_tiny.orders",
                            List.of(column("orderkey", "integer", null, false, "o_orderkey"),
                                    column("custkey", "integer", null, false, "o_custkey")))))
            .build();

    @Override
    protected TestingWrenServer createWrenServer()
    {
        try {
            mdlDir = Files.createTempDirectory("wrenmdls");
            wrenMDLFilePath = mdlDir.resolve("wrenmdl.json");
            Files.write(wrenMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(initial));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("bigquery.metadata.schema.prefix", format("test_%s_", randomIntString()))
                .put("wren.directory", mdlDir.toAbsolutePath().toString())
                .put("wren.datasource.type", "bigquery");

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testDeploy()
            throws Exception
    {
        CheckOutputDto startUp = getDeployStatus();
        assertThat(startUp.getStatus()).isEqualTo(CheckOutputDto.Status.READY);
        assertThat(startUp.getVersion()).isNull();
        assertThat(getCurrentManifest().getModels().get(0).getColumns().size()).isEqualTo(1);
        assertThatNoException().isThrownBy(() -> deployMDL(new DeployInputDto(updated, "abcd")));
        CheckOutputDto afterDeploy = getDeployStatus();
        assertThat(afterDeploy.getVersion()).isEqualTo("abcd");
        assertThat(getCurrentManifest().getModels().get(0).getColumns().size()).isEqualTo(2);
        waitUntilReady();

        assertThatNoException().isThrownBy(() -> deployMDL(new DeployInputDto(initial, "1234")));
        CheckOutputDto afterDeploy2 = getDeployStatus();
        assertThat(afterDeploy2.getVersion()).isEqualTo("1234");
        assertThat(getCurrentManifest().getModels().get(0).getColumns().size()).isEqualTo(1);
        waitUntilReady();

        assertThat(requireNonNull(mdlDir.resolve(ARCHIVED).toFile().listFiles()).length).isEqualTo(2);
        assertThatNoException().isThrownBy(() -> preview(new PreviewDto(null, "select orderkey from Orders limit 100", null)));
    }

    @Test
    public void testPreview()
    {
        Manifest previewManifest = Manifest.builder()
                .setCatalog("canner-cml")
                .setSchema("tpch_tiny")
                .setModels(List.of(
                        model("Customer", "SELECT * FROM \"canner-cml\".tpch_tiny.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey")))))
                .build();

        PreviewDto testDefaultDto = new PreviewDto(previewManifest, "select custkey from Customer", null);
        QueryResultDto testDefault = preview(testDefaultDto);
        assertThat(testDefault.getData().size()).isEqualTo(100);
        assertThat(testDefault.getColumns().size()).isEqualTo(1);
        assertThat(testDefault.getColumns().get(0).getName()).isEqualTo("custkey");
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo(BigIntType.BIGINT);

        PreviewDto testDefaultDto1 = new PreviewDto(previewManifest, "select custkey from Customer limit 200", null);
        QueryResultDto preview1 = preview(testDefaultDto1);
        assertThat(preview1.getData().size()).isEqualTo(100);
        assertThat(preview1.getColumns().size()).isEqualTo(1);

        PreviewDto testDefaultDto2 = new PreviewDto(previewManifest, "select custkey from Customer limit 200", 150L);
        QueryResultDto preview2 = preview(testDefaultDto2);
        assertThat(preview2.getData().size()).isEqualTo(150);
        assertThat(preview2.getColumns().size()).isEqualTo(1);

        assertWebApplicationException(() -> preview(new PreviewDto(previewManifest, "select orderkey from Orders limit 100", null)))
                .hasErrorMessageMatches(".*Table \"Orders\" must be qualified with a dataset.*");
    }
}
