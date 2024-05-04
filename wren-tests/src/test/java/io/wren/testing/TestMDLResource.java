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
import com.google.common.io.Resources;
import com.google.inject.Key;
import io.wren.base.dto.Column;
import io.wren.base.dto.JoinType;
import io.wren.base.dto.Manifest;
import io.wren.base.type.IntegerType;
import io.wren.main.connector.duckdb.DuckDBMetadata;
import io.wren.main.validation.ColumnIsValid;
import io.wren.main.validation.ValidationResult;
import io.wren.main.web.dto.CheckOutputDto;
import io.wren.main.web.dto.DeployInputDto;
import io.wren.main.web.dto.DryPlanDto;
import io.wren.main.web.dto.PreviewDto;
import io.wren.main.web.dto.QueryResultDto;
import io.wren.main.web.dto.ValidateDto;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.wren.base.client.duckdb.FileUtil.ARCHIVED;
import static io.wren.base.config.WrenConfig.DataSourceType.DUCKDB;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static io.wren.base.config.WrenConfig.WREN_DIRECTORY;
import static io.wren.base.config.WrenConfig.WREN_ENABLE_DYNAMIC_FIELDS;
import static io.wren.base.dto.Column.caluclatedColumn;
import static io.wren.base.dto.Column.column;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static io.wren.base.dto.Model.model;
import static io.wren.base.dto.Relationship.relationship;
import static io.wren.main.validation.ColumnIsValid.COLUMN_IS_VALID;
import static io.wren.testing.WebApplicationExceptionAssert.assertWebApplicationException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestMDLResource
        extends RequireWrenServer
{
    private Path wrenMDLFilePath;
    private Path mdlDir;
    private Manifest initial = Manifest.builder()
            .setCatalog("wrenai")
            .setSchema("tpch")
            .setModels(List.of(
                    model("Orders", "SELECT * FROM tpch.orders", List.of(column("orderkey", "integer", null, false, "o_orderkey")))))
            .build();

    private Manifest updated = Manifest.builder()
            .setCatalog("wrenai")
            .setSchema("tpch")
            .setModels(List.of(
                    model("Orders", "SELECT * FROM tpch.orders",
                            List.of(column("orderkey", "integer", null, false, "o_orderkey"),
                                    column("custkey", "integer", null, false, "o_custkey")))))
            .build();

    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
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
                .put(WREN_DIRECTORY, mdlDir.toAbsolutePath().toString())
                .put(WREN_DATASOURCE_TYPE, DUCKDB.name())
                .put(WREN_ENABLE_DYNAMIC_FIELDS, "true");

        TestingWrenServer testing = TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
        initDuckDB(testing);
        return testing;
    }

    protected void initDuckDB(TestingWrenServer wrenServer)
            throws Exception
    {
        ClassLoader classLoader = getClass().getClassLoader();
        String initSQL = Resources.toString(requireNonNull(classLoader.getResource("duckdb/init.sql")).toURI().toURL(), UTF_8);
        initSQL = initSQL.replaceAll("basePath", requireNonNull(classLoader.getResource("tpch/data")).getPath());
        DuckDBMetadata metadata = wrenServer.getInstance(Key.get(DuckDBMetadata.class));
        metadata.setInitSQL(initSQL);
        metadata.reload();
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
                .setCatalog("wrenai")
                .setSchema("tpch")
                .setModels(List.of(
                        model("Customer", "SELECT * FROM tpch.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey")))))
                .build();

        PreviewDto testDefaultDto = new PreviewDto(previewManifest, "select custkey from Customer", null);
        QueryResultDto testDefault = preview(testDefaultDto);
        assertThat(testDefault.getData().size()).isEqualTo(100);
        assertThat(testDefault.getColumns().size()).isEqualTo(1);
        assertThat(testDefault.getColumns().get(0).getName()).isEqualTo("custkey");
        assertThat(testDefault.getColumns().get(0).getType()).isEqualTo(IntegerType.INTEGER);

        PreviewDto testDefaultDto1 = new PreviewDto(previewManifest, "select custkey from Customer limit 200", null);
        QueryResultDto preview1 = preview(testDefaultDto1);
        assertThat(preview1.getData().size()).isEqualTo(100);
        assertThat(preview1.getColumns().size()).isEqualTo(1);

        PreviewDto testDefaultDto2 = new PreviewDto(previewManifest, "select custkey from Customer limit 200", 150L);
        QueryResultDto preview2 = preview(testDefaultDto2);
        assertThat(preview2.getData().size()).isEqualTo(150);
        assertThat(preview2.getColumns().size()).isEqualTo(1);

        assertWebApplicationException(() -> preview(new PreviewDto(previewManifest, "select orderkey from Orders limit 100", null)))
                .hasErrorMessageMatches(".*Orders does not exist.*\n.*\n.*\n.*");
    }

    @Test
    public void testDryRunAndDryPlan()
    {
        Manifest previewManifest = Manifest.builder()
                .setCatalog("wrenai")
                .setSchema("tpch")
                .setModels(List.of(
                        model("Customer", "SELECT * FROM tpch.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey"),
                                        column("name", "varchar", null, false, "c_name"))),
                        model("Orders", "SELECT * FROM tpch.orders",
                                List.of(column("orderkey", "integer", null, false, "o_orderkey"),
                                        column("custkey", "integer", null, false, "o_custkey"),
                                        column("customer", "Customer", "CustomerOrders", false),
                                        caluclatedColumn("customer_name", "varchar", "customer.name")),
                                "orderkey")))
                .setRelationships(List.of(relationship("CustomerOrders", List.of("Customer", "Orders"), JoinType.ONE_TO_MANY, "Customer.custkey = Orders.custkey")))
                .build();

        PreviewDto testDefaultDto1 = new PreviewDto(previewManifest, "select orderkey from Orders limit 200", null);
        List<Column> dryRun = dryRun(testDefaultDto1);
        assertThat(dryRun.size()).isEqualTo(1);
        assertThat(dryRun.get(0).getName()).isEqualTo("orderkey");

        DryPlanDto dryPlanDto = new DryPlanDto(previewManifest, "select orderkey from Orders limit 200", false);
        String dryPlan = dryPlan(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "Orders" AS (
                   SELECT
                     "Orders"."orderkey" "orderkey"
                   , "Orders"."custkey" "custkey"
                   FROM
                     (
                      SELECT
                        "Orders"."orderkey" "orderkey"
                      , "Orders"."custkey" "custkey"
                      FROM
                        (
                         SELECT
                           o_orderkey "orderkey"
                         , o_custkey "custkey"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.orders
                         )  "Orders"
                      )  "Orders"
                   )  "Orders"
                )\s
                SELECT orderkey
                FROM
                  Orders
                LIMIT 200
                """);

        dryPlanDto = new DryPlanDto(previewManifest, "select orderkey from Orders limit 200", true);
        dryPlan = dryPlan(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "Orders" AS (
                   SELECT
                     "Orders"."orderkey" "orderkey"
                   , "Orders"."custkey" "custkey"
                   FROM
                     (
                      SELECT
                        "Orders"."orderkey" "orderkey"
                      , "Orders"."custkey" "custkey"
                      FROM
                        (
                         SELECT
                           o_orderkey "orderkey"
                         , o_custkey "custkey"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.orders
                         )  "Orders"
                      )  "Orders"
                   )  "Orders"
                )\s
                SELECT orderkey
                FROM
                  Orders
                LIMIT 200
                """);

        dryPlanDto = new DryPlanDto(previewManifest, "select customer_name from Orders limit 200", false);
        dryPlan = dryPlan(dryPlanDto);
        assertThat(dryPlan).isEqualTo("""
                WITH
                  "Customer" AS (
                   SELECT
                     "Customer"."custkey" "custkey"
                   , "Customer"."name" "name"
                   FROM
                     (
                      SELECT
                        "Customer"."custkey" "custkey"
                      , "Customer"."name" "name"
                      FROM
                        (
                         SELECT
                           c_custkey "custkey"
                         , c_name "name"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.customer
                         )  "Customer"
                      )  "Customer"
                   )  "Customer"
                )\s
                , "Orders" AS (
                   SELECT
                     "Orders"."orderkey" "orderkey"
                   , "Orders"."custkey" "custkey"
                   , "Orders_relationsub"."customer_name" "customer_name"
                   FROM
                     ((
                      SELECT
                        "Orders"."orderkey" "orderkey"
                      , "Orders"."custkey" "custkey"
                      FROM
                        (
                         SELECT
                           o_orderkey "orderkey"
                         , o_custkey "custkey"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.orders
                         )  "Orders"
                      )  "Orders"
                   )  "Orders"
                   LEFT JOIN (
                      SELECT
                        "Orders"."orderkey"
                      , "Customer"."name" "customer_name"
                      FROM
                        ((
                         SELECT
                           o_orderkey "orderkey"
                         , o_custkey "custkey"
                         FROM
                           (
                            SELECT *
                            FROM
                              tpch.orders
                         )  "Orders"
                      )  "Orders"
                      LEFT JOIN "Customer" ON ("Customer"."custkey" = "Orders"."custkey"))
                   )  "Orders_relationsub" ON ("Orders"."orderkey" = "Orders_relationsub"."orderkey"))
                )\s
                SELECT customer_name
                FROM
                  Orders
                LIMIT 200
                """);
    }

    @Test
    public void testValidation()
    {
        List<ValidationResult> validations = validate(COLUMN_IS_VALID, new ValidateDto(null, ColumnIsValid.parameters("Orders", "orderkey")));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid:Orders:orderkey");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.PASS);

        validations = validate(COLUMN_IS_VALID, new ValidateDto(null, ColumnIsValid.parameters("Orders", "notfound")));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid:Orders:notfound");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.FAIL);
        assertThat(validations.getFirst().getMessage()).isNotEmpty();

        validations = validate(COLUMN_IS_VALID, new ValidateDto(null, ColumnIsValid.parameters(null, "orderkey")));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.ERROR);
        assertThat(validations.getFirst().getMessage()).isEqualTo("Model name is required");

        validations = validate(COLUMN_IS_VALID, new ValidateDto(null, ColumnIsValid.parameters("", "orderkey")));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.ERROR);
        assertThat(validations.getFirst().getMessage()).isEqualTo("Model name is required");

        validations = validate(COLUMN_IS_VALID, new ValidateDto(null, ColumnIsValid.parameters("Orders", null)));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid:Orders");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.ERROR);
        assertThat(validations.getFirst().getMessage()).isEqualTo("Column name is required");

        validations = validate(COLUMN_IS_VALID, new ValidateDto(null, ColumnIsValid.parameters("Orders", "")));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid:Orders");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.ERROR);
        assertThat(validations.getFirst().getMessage()).isEqualTo("Column name is required");

        validations = validate(COLUMN_IS_VALID, new ValidateDto(null, ColumnIsValid.parameters(null, null)));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.ERROR);
        assertThat(validations.getFirst().getMessage()).isEqualTo("Model name is required");

        validations = validate(COLUMN_IS_VALID, new ValidateDto(null, null));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.ERROR);
        assertThat(validations.getFirst().getMessage()).isEqualTo("Model name is required");

        validations = validate(COLUMN_IS_VALID, null);
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.ERROR);
        assertThat(validations.getFirst().getMessage()).isEqualTo("Model name is required");

        assertWebApplicationException(() -> validate("notfound", new ValidateDto(null, ColumnIsValid.parameters("Orders", ""))))
                .hasHTTPStatus(404)
                .hasErrorMessageMatches(".*Validation rule not found: .*");
    }

    @Test
    public void testValidationWithMDL()
    {
        Manifest previewManifest = Manifest.builder()
                .setCatalog("wrenai")
                .setSchema("tpch")
                .setModels(List.of(
                        model("Customer", "SELECT * FROM tpch.customer",
                                List.of(column("custkey", "integer", null, false, "c_custkey")))))
                .build();

        List<ValidationResult> validations = validate(COLUMN_IS_VALID, new ValidateDto(previewManifest, ColumnIsValid.parameters("Customer", "custkey")));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid:Customer:custkey");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.PASS);

        validations = validate(COLUMN_IS_VALID, new ValidateDto(previewManifest, ColumnIsValid.parameters("Orders", "custkey")));
        assertThat(validations.size()).isEqualTo(1);
        assertThat(validations.getFirst().getName()).isEqualTo("column_is_valid:Orders:custkey");
        assertThat(validations.getFirst().getStatus()).isEqualTo(ValidationResult.Status.FAIL);
        assertThat(validations.getFirst().getMessage()).isNotEmpty();
    }
}
