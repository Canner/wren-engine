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
import io.accio.base.AccioMDL;
import io.accio.base.dto.Manifest;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetadataQuery
        extends AbstractWireProtocolTest
{
    @Override
    protected String getDefaultCatalog()
    {
        return "canner-cml";
    }

    @Override
    protected String getDefaultSchema()
    {
        return "tiny_tpch";
    }

    @Override
    protected Optional<String> getAccioMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("bigquery/TestWireProtocolWithBigquery.json")).getPath());
    }

    @Override
    protected TestingAccioServer createAccioServer()
    {
        Path dir;
        try {
            dir = Files.createTempDirectory(getAccioDirectory());
            if (getAccioMDLPath().isPresent()) {
                Files.copy(Path.of(getAccioMDLPath().get()), dir.resolve("mdl.json"));
            }
            else {
                Files.write(dir.resolve("manifest.json"), Manifest.MANIFEST_JSON_CODEC.toJsonBytes(AccioMDL.EMPTY.getManifest()));
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("accio.datasource.type", "DUCKDB")
                .put("accio.directory", dir.toString());

        return TestingAccioServer.builder()
                .setRequiredConfigs(properties.build())
                .build();
    }

    @Test
    public void testBasic()
            throws Exception
    {
        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT typname FROM pg_type WHERE oid = 14");
            result.next();
            assertThat(result.getString(1)).isEqualTo("bigint");
        }

        try (Connection conn = createConnection(); Statement stmt = conn.createStatement()) {
            ResultSet result = stmt.executeQuery("SELECT 'Orders'::regclass");
            result.next();
            assertThat(result.getString(1)).isEqualTo("Orders");
        }
    }

    @Test
    public void testPreparedStatement()
            throws Exception
    {
        try (Connection conn = createConnection()) {
            PreparedStatement stmt = conn.prepareStatement("SELECT typname FROM pg_type WHERE oid = ?");
            stmt.setInt(1, 14);
            ResultSet result = stmt.executeQuery();
            result.next();
            assertThat(result.getString(1)).isEqualTo("bigint");
        }
    }
}
