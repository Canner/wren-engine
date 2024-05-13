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
import io.wren.base.dto.Manifest;
import io.wren.testing.AbstractFunctionTest;
import io.wren.testing.TestingWrenServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.wren.base.Utils.randomIntString;
import static io.wren.base.dto.Manifest.MANIFEST_JSON_CODEC;
import static java.lang.String.format;
import static java.lang.System.getenv;

public class TestFunctionBigQuery
        extends AbstractFunctionTest
{
    @Override
    protected TestingWrenServer createWrenServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("bigquery.metadata.schema.prefix", format("test_%s_", randomIntString()))
                .put("wren.datasource.type", "bigquery");
        Path mdlDir;
        try {
            mdlDir = Files.createTempDirectory("wrenmdls");
            Path wrenMDLFilePath = mdlDir.resolve("wrenmdl.json");
            Files.write(wrenMDLFilePath, MANIFEST_JSON_CODEC.toJsonBytes(Manifest.builder().setCatalog("wren").setSchema("test").build()));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        return TestingWrenServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }
}
