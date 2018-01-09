package com.presto.samples.extract;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Created by ethan
 * 17-10-18
 */
public class ExtractConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "extract";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new ExtractHandleResolver();
    }

    @Override
    public Connector create(String s, Map<String, String> map, ConnectorContext connectorContext) {
        requireNonNull(map, "requiredConfig is null");
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new ExtractModule(s,connectorContext.getNodeManager()));
            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(map)
                    .initialize();

            return injector.getInstance(ExtractConnector.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
