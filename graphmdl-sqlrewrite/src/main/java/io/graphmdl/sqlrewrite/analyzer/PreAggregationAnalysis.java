package io.graphmdl.sqlrewrite.analyzer;

import io.graphmdl.base.CatalogSchemaTableName;

import java.util.HashSet;
import java.util.Set;

public class PreAggregationAnalysis
{
    private final Set<CatalogSchemaTableName> tables = new HashSet<>();
    private final Set<CatalogSchemaTableName> preAggregationTables = new HashSet<>();

    public void addTable(CatalogSchemaTableName tableName)
    {
        tables.add(tableName);
    }

    public void addPreAggregationTables(CatalogSchemaTableName preAggregationTables)
    {
        this.preAggregationTables.add(preAggregationTables);
    }

    public boolean onlyPreAggregationTables()
    {
        return preAggregationTables.size() > 0 && tables.equals(preAggregationTables);
    }
}
