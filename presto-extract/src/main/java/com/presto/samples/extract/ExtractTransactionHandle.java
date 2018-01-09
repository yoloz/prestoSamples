package com.presto.samples.extract;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

/**
 * Created by ethan
 * 17-10-19
 */
public enum ExtractTransactionHandle implements ConnectorTransactionHandle {
    INSTANCE
}