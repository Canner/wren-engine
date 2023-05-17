package io.graphmdl.preaggregation;

import java.util.Optional;

public interface PreAggregationStorageConfig
{
    String generateDuckdbParquetStatement(String path, String tableName);
}
