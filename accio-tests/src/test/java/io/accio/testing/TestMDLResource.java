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

import com.google.common.collect.ImmutableMap;
import io.accio.base.dto.Manifest;
import io.accio.main.web.dto.CheckOutputDto;
import io.accio.main.web.dto.PreviewDto;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.accio.base.Utils.randomIntString;
import static io.accio.base.dto.Column.column;
import static io.accio.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.accio.base.dto.Model.model;
import static io.accio.testing.WebApplicationExceptionAssert.assertWebApplicationException;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestMDLResource
        extends RequireAccioServer
{
    private Path accioMDLFilePath;
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
    protected TestingAccioServer createAccioServer()
    {
        try {
            mdlDir = Files.createTempDirectory("acciomdls");
            accioMDLFilePath = mdlDir.resolve("acciomdl.json");
            Files.write(accioMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(initial));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("bigquery.metadata.schema.prefix", format("test_%s_", randomIntString()))
                .put("accio.directory", mdlDir.toAbsolutePath().toString())
                .put("accio.datasource.type", "bigquery");

        return TestingAccioServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testDeploy()
            throws Exception
    {
        CheckOutputDto startUp = getDeployStatus();
        assertThat(startUp.getManifest().getModels().get(0).getColumns().size()).isEqualTo(1);
        assertThatNoException().isThrownBy(() -> deployMDL(updated));
        CheckOutputDto afterDeploy = getDeployStatus();
        assertThat(afterDeploy.getManifest().getModels().get(0).getColumns().size()).isEqualTo(2);
        waitUntilReady();

        assertThatNoException().isThrownBy(() -> deployMDL(initial));
        CheckOutputDto afterDeploy2 = getDeployStatus();
        assertThat(afterDeploy2.getManifest().getModels().get(0).getColumns().size()).isEqualTo(1);
        waitUntilReady();

        assertThat(requireNonNull(mdlDir.resolve("archive").toFile().listFiles()).length).isEqualTo(2);
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
        List<Object[]> testDefault = preview(testDefaultDto);
        assertThat(testDefault.size()).isEqualTo(100);
        assertThat(testDefault.get(0).length).isEqualTo(1);

        PreviewDto testDefaultDto1 = new PreviewDto(previewManifest, "select custkey from Customer limit 200", null);
        List<Object[]> preview1 = preview(testDefaultDto1);
        assertThat(preview1.size()).isEqualTo(100);
        assertThat(preview1.get(0).length).isEqualTo(1);

        PreviewDto testDefaultDto2 = new PreviewDto(previewManifest, "select custkey from Customer limit 200", 150L);
        List<Object[]> preview2 = preview(testDefaultDto2);
        assertThat(preview2.size()).isEqualTo(150);
        assertThat(preview2.get(0).length).isEqualTo(1);

        assertWebApplicationException(() -> preview(new PreviewDto(previewManifest, "select orderkey from Orders limit 100", null)))
                .hasErrorMessageMatches(".*Table \"Orders\" must be qualified with a dataset.*");
    }
}
