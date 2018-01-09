package com.presto.samples.extract;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Created by ethan
 * 17-10-18
 */
public class ExtractSplitManager implements ConnectorSplitManager {

    private final String connectorId;
    private final String connectionUrl;
    private final Properties connectionProperties;

    @Inject
    public ExtractSplitManager(ExtractConnectorId connectorId, ExtractConfig config) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        requireNonNull(config, "config is null");
        connectionUrl = config.getJdbcUrl();
        connectionProperties = new Properties();
        if (config.getUserName() != null) {
            connectionProperties.setProperty("user", config.getUserName());
        }
        if (config.getPwd() != null) {
            connectionProperties.setProperty("password", config.getPwd());
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout) {
        ExtractTableLayoutHandle layoutHandle = (ExtractTableLayoutHandle) layout;
        ExtractTableHandle tableHandle = layoutHandle.getTable();
        ExtractSplit extractSplit = new ExtractSplit(
                connectorId,
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                connectionUrl,
                connectionProperties,
                layoutHandle.getTupleDomain());
        return new FixedSplitSource(ImmutableList.of(extractSplit));
    }
}
