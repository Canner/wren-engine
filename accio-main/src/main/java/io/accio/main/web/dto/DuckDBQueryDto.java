package io.accio.main.web.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DuckDBQueryDto
{
    private final List<Column> columns;
    private final List<List<Object>> rows;

    @JsonCreator
    public DuckDBQueryDto(
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("rows") List<List<Object>> rows)
    {
        this.columns = columns;
        this.rows = rows;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<List<Object>> getRows()
    {
        return rows;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columns)
                .add("rows", rows)
                .toString();
    }

    public static class Builder
    {
        private List<Column> columns;
        private List<List<Object>> rows;

        public Builder columns(List<Column> columns)
        {
            this.columns = columns;
            return this;
        }

        public Builder rows(List<List<Object>> rows)
        {
            this.rows = rows;
            return this;
        }

        public DuckDBQueryDto build()
        {
            return new DuckDBQueryDto(columns, rows);
        }
    }

    public static class Column
    {
        private final String name;
        private final String type;

        public static Column of(String name, String type)
        {
            return new Column(name, type);
        }

        @JsonCreator
        public Column(
                @JsonProperty("name") String name,
                @JsonProperty("type") String type)
        {
            this.name = name;
            this.type = type;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("type", type)
                    .toString();
        }
    }
}
