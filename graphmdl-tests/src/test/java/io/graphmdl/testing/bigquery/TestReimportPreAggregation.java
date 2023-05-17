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
import static io.graphmdl.base.GraphMDL.fromJson;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReimportPreAggregation
        extends AbstractPreAggregationTest
{
    @Override
    protected Optional<String> getGraphMDLPath()
    {
        return Optional.of(requireNonNull(getClass().getClassLoader().getResource("pre_agg/pre_agg_reimport_1_mdl.json")).getPath());
    }

    @Test
    public void testReimportPreAggregation()
            throws IOException
    {
        String beforeMetricName = "Revenue";
        CatalogSchemaTableName beforeCatalogSchemaTableName = new CatalogSchemaTableName("canner-cml", "tpch_tiny", beforeMetricName);
        String beforeMappingName = getDefaultMetricTablePair(beforeMetricName).getRequiredTableName();
        assertPreAggregation(beforeMetricName);

        String reimportJson =
                Files.readString(Path.of(requireNonNull(getClass().getClassLoader().getResource("pre_agg/pre_agg_reimport_2_mdl.json")).getFile()));
        preAggregationManager.importPreAggregation(fromJson(reimportJson));
        assertPreAggregation("Revenue_After");

        List<Object[]> tables = queryDuckdb("show tables");
        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).doesNotContain(beforeMappingName);
        assertThat(preAggregationManager.metricScheduledFutureExists(beforeCatalogSchemaTableName)).isFalse();
        assertThatThrownBy(() -> getDefaultMetricTablePair(beforeMappingName).getRequiredTableName()).isInstanceOf(NullPointerException.class);
    }

    private void assertPreAggregation(String metricName)
    {
        CatalogSchemaTableName mapping = new CatalogSchemaTableName("canner-cml", "tpch_tiny", metricName);
        String mappingName = getDefaultMetricTablePair(metricName).getRequiredTableName();
        List<Object[]> tables = queryDuckdb("show tables");
        Set<String> tableNames = tables.stream().map(table -> table[0].toString()).collect(toImmutableSet());
        assertThat(tableNames).contains(mappingName);
        assertThat(preAggregationManager.metricScheduledFutureExists(mapping)).isTrue();
    }
}
