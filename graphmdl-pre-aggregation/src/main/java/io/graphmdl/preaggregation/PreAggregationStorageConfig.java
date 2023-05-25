package io.graphmdl.preaggregation;

public interface PreAggregationStorageConfig
{
    String generateDuckdbParquetStatement(String path, String tableName);
}
