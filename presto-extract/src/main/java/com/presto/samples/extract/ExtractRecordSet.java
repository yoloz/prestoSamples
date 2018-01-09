package com.presto.samples.extract;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Created by ethan
 * 17-10-19
 */
public class ExtractRecordSet implements RecordSet {

    private final ExtractClient extractClient;
    private final List<ExtractColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final ExtractSplit split;

    public ExtractRecordSet(ExtractClient extractClient, ExtractSplit split, List<ExtractColumnHandle> columnHandles) {
        this.extractClient = requireNonNull(extractClient, "extractClient is null");
        this.split = requireNonNull(split, "split is null");

        requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ExtractColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new ExtractRecordCursor(extractClient, split, columnHandles);
    }
}
