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

package io.wren.testing.bigquery;

import com.google.common.collect.ImmutableMap;
import io.wren.base.config.WrenConfig;
import io.wren.base.dto.Manifest;
import io.wren.testing.TestingWrenServer;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static io.wren.base.Utils.randomIntString;
import static io.wren.base.config.BigQueryConfig.BIGQUERY_CRENDITALS_KEY;
import static io.wren.base.config.BigQueryConfig.BIGQUERY_PROJECT_ID;
import static io.wren.base.config.ConfigManager.ConfigEntry.configEntry;
import static io.wren.base.config.WrenConfig.WREN_DATASOURCE_TYPE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeployBigQueryWithFakeConfigs
        extends AbstractWireProtocolTestWithBigQuery
{
    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", "fake")
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", "fake")
                .put("bigquery.metadata.schema.prefix", format("test_%s_", randomIntString()))
                .put("pg-wire-protocol.auth.file", requireNonNull(getClass().getClassLoader().getResource("accounts")).getPath())
                .put("wren.datasource.type", "bigquery");

        try {
            Path dir = Files.createTempDirectory(getWrenDirectory());
            if (getWrenMDLPath().isPresent()) {
                Files.copy(Path.of(getWrenMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(DEFAULT_MANIFEST));
            }
            properties.put("wren.directory", dir.toString());
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testDeployBigQueryWithFakeConfigs()
    {
        assertThat(getConfig(WREN_DATASOURCE_TYPE)).isEqualTo(configEntry(WREN_DATASOURCE_TYPE, WrenConfig.DataSourceType.BIGQUERY.name()));
        assertThat(getConfig(BIGQUERY_PROJECT_ID)).isEqualTo(configEntry(BIGQUERY_PROJECT_ID, "fake"));
        assertThat(getConfig(BIGQUERY_CRENDITALS_KEY)).isEqualTo(configEntry(BIGQUERY_CRENDITALS_KEY, "fake"));
    }
}
