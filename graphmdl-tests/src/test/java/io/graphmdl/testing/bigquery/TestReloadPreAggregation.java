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
package io.graphmdl.testing.bigquery;

import io.graphmdl.base.CatalogSchemaTableName;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReloadPreAggregation
        extends AbstractPreAggregationTest
{
    private Path graphMDLFilePath;

    @Override
    protected Optional<String> getGraphMDLPath()
    {
        try {
            graphMDLFilePath = Files.createTempFile("graphmdl", ".json");
            rewriteFile("pre_agg/pre_agg_reload_1_mdl.json");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Optional.of(graphMDLFilePath.toString());
    }

    @Test
    public void testReloadPreAggregation()
            throws IOException
    {
        String beforeName = "Revenue";
        CatalogSchemaTableName beforeCatalogSchemaTableName = new CatalogSchemaTableName("canner-cml", "tpch_tiny", beforeName);
        String beforeMappingName = getDefaultPreAggregationInfoPair(beforeName).getRequiredTableName();
        assertPreAggregation(beforeName);

        rewriteFile("pre_agg/pre_agg_reload_2_mdl.json");
        reloadGraphMDL();
        reloadPreAggregation();

        assertPreAggregation("Revenue_After");

        List<Object[]> tables = queryDuckdb("show tables");
        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).doesNotContain(beforeMappingName);
        assertThat(preAggregationManager.preAggregationScheduledFutureExists(beforeCatalogSchemaTableName)).isFalse();
        assertThatThrownBy(() -> getDefaultPreAggregationInfoPair(beforeMappingName).getRequiredTableName()).isInstanceOf(NullPointerException.class);
    }

    private void assertPreAggregation(String name)
    {
        CatalogSchemaTableName mapping = new CatalogSchemaTableName("canner-cml", "tpch_tiny", name);
        String mappingName = getDefaultPreAggregationInfoPair(name).getRequiredTableName();
        List<Object[]> tables = queryDuckdb("show tables");
        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).contains(mappingName);
        assertThat(preAggregationManager.preAggregationScheduledFutureExists(mapping)).isTrue();
    }

    private void rewriteFile(String resourcePath)
            throws IOException
    {
        Files.copy(Path.of(requireNonNull(getClass().getClassLoader().getResource(resourcePath)).getPath()), graphMDLFilePath, REPLACE_EXISTING);
    }
}
