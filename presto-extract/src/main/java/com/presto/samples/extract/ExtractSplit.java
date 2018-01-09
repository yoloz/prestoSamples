package com.presto.samples.extract;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Created by ethan
 * 17-10-18
 */
public class ExtractSplit implements ConnectorSplit {

    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public ExtractSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("connectionUrl") String connectionUrl,
            @JsonProperty("connectionProperties") Properties connectionProperties,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain
    )

    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "table name is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = requireNonNull(connectionProperties, "connectionProperties is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName() {
        return catalogName;
    }

    @JsonProperty
    @Nullable
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getConnectionUrl() {
        return connectionUrl;
    }

    @JsonProperty
    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
