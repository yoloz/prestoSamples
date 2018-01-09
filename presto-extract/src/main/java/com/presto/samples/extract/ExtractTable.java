package com.presto.samples.extract;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Created by ethan
 * 17-10-19
 */
public class ExtractTable {

    private final String name;
    private final List<ExtractColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final List<URI> sources;

    @JsonCreator
    public ExtractTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<ExtractColumn> columns,
            @JsonProperty("sources") List<URI> sources) {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (ExtractColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public List<ExtractColumn> getColumns() {
        return columns;
    }

    @JsonProperty
    public List<URI> getSources() {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata() {
        return columnsMetadata;
    }
}
