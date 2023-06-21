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
import com.google.inject.Key;
import io.accio.base.dto.Manifest;
import io.accio.main.AccioMetastore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.System.getenv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReload
        extends RequireAccioServer
{
    private Path accioMDLFilePath;

    @Override
    protected TestingAccioServer createAccioServer()
    {
        try {
            accioMDLFilePath = Files.createTempFile("acciomdl", ".json");
            Manifest manifest = Manifest.builder().setCatalog("catalog").setSchema("schema").build();
            Files.write(accioMDLFilePath, jsonCodec(Manifest.class).toJsonBytes(manifest));
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", getenv("TEST_BIG_QUERY_PROJECT_ID"))
                .put("bigquery.location", "asia-east1")
                .put("bigquery.credentials-key", getenv("TEST_BIG_QUERY_CREDENTIALS_BASE64_JSON"))
                .put("accio.file", accioMDLFilePath.toAbsolutePath().toString())
                .put("accio.datasource.type", "bigquery");

        return TestingAccioServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testReload()
    {
        AccioMetastore accioMetastore = server().getInstance(Key.get(AccioMetastore.class));
        assertThatThrownBy(() -> rewriteFileAndRequestReload(""));
        assertThatNoException().isThrownBy(() -> rewriteFileAndRequestReload(Manifest.builder().setCatalog("foo").setSchema("bar").build()));
        assertThat(accioMetastore.getAccioMDL().getCatalog()).isEqualTo("foo");
        assertThat(accioMetastore.getAccioMDL().getSchema()).isEqualTo("bar");
    }

    private void rewriteFileAndRequestReload(Manifest manifest)
            throws IOException
    {
        rewriteFileAndRequestReload(jsonCodec(Manifest.class).toJson(manifest));
    }

    private void rewriteFileAndRequestReload(String accioMDLFileContent)
            throws IOException
    {
        Files.write(accioMDLFilePath, accioMDLFileContent.getBytes(UTF_8));
        reloadAccioMDL();
    }
}
