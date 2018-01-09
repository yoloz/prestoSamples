package com.presto.samples.extract;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;

import static com.presto.samples.extract.ExtractTransactionHandle.INSTANCE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Created by ethan
 * 17-10-18
 */
public class ExtractConnector implements Connector {
    private static final Logger log = Logger.get(ExtractConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final ExtractMetadata metadata;
    private final ExtractSplitManager splitManager;
    private final ExtractRecordSetProvider recordSetProvider;
    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public ExtractConnector(LifeCycleManager lifeCycleManager,
                            ExtractMetadata metadata,
                            ExtractSplitManager splitManager,
                            ExtractRecordSetProvider recordSetProvider,
                            ExtractTableProperties tableProperties) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.tableProperties = ImmutableList.copyOf(requireNonNull(tableProperties.getTableProperties(), "tableProperties is null"));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
//        System.out.println("-----这个不实现则不会执行任何操作");
        return INSTANCE;
    }

    public void rollback(ConnectorTransactionHandle transaction) {
//        ExtractMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        metadata.rollback();
    }

    /**
     * Guaranteed to be called at most once per transaction. The returned metadata will only be accessed
     * in a single threaded context.
     *
     * @param transactionHandle transactionHandle
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }

    @Override
    public final void shutdown() {
        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }

    }
}
