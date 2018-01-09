package com.presto.samples.extract;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

/**
 * Created by ethan
 * 17-10-18
 */
public class ExtractRecordSetProvider implements ConnectorRecordSetProvider {

    private final ExtractClient extractClient;

    @Inject
    public ExtractRecordSetProvider(ExtractClient extractClient) {
        this.extractClient = extractClient;
    }



    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        ExtractSplit jdbcSplit = (ExtractSplit) split;

        ImmutableList.Builder<ExtractColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((ExtractColumnHandle) handle);
        }

        return new ExtractRecordSet(extractClient, jdbcSplit, handles.build());
    }
}
