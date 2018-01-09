package com.presto.samples.extract;

import com.facebook.presto.spi.NodeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;


/**
 * Created by ethan
 * 17-10-18
 */
public class ExtractModule implements Module {

    private final String connectorId;
    private final NodeManager nodeManager;

    public ExtractModule(String connectorId,NodeManager nodeManager) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(ExtractConnector.class).in(Scopes.SINGLETON);
        binder.bind(ExtractConnectorId.class).toInstance(new ExtractConnectorId(connectorId));
        binder.bind(ExtractMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ExtractClient.class).in(Scopes.SINGLETON);
        binder.bind(ExtractSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ExtractRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ExtractTableProperties.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ExtractConfig.class);
    }
}
