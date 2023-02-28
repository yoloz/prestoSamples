package com.presto.samples.extract;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Created by ethan
 * 17-10-18
 */
public class ExtractMetadata implements ConnectorMetadata {

    private final Logger logger = Logger.get(ExtractMetadata.class);

    private final String connectorId;

    private final ExtractClient extractClient;

    //创建表之类操作有begin和finish操作，begin和finish没有都成功的时候可以rollback
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    @Inject
    public ExtractMetadata(ExtractConnectorId connectorId, ExtractClient extractClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.extractClient = requireNonNull(extractClient, "client is null");
    }

    /**
     * Returns the schemas provided by this connector.
     *
     * @param session
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.copyOf(extractClient.getSchemaNames());
    }


    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     *
     * @param session
     * @param tableName
     */
    @Override
    public ExtractTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return extractClient.getTableHandle(tableName);
    }

    /**
     * Return a list of table layouts that satisfy the given constraint.
     * <p>
     * For each layout, connectors must return an "unenforced constraint" representing the part of the constraint summary that isn't guaranteed by the layout.
     *
     * @param session
     * @param table
     * @param constraint
     * @param desiredColumns
     */
    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
                                                            ConnectorTableHandle table,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns) {
        ExtractTableHandle tableHandle = (ExtractTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new ExtractTableLayoutHandle(
                tableHandle,
                constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    /**
     * Return the metadata for the specified table handle.
     *
     * @param session
     * @param table
     * @throws RuntimeException if table handle is no longer valid
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        ExtractTableHandle extractTableHandle = (ExtractTableHandle) table;
        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (ExtractColumnHandle column : extractClient.getColumns(extractTableHandle)) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(extractTableHandle.getSchemaTableName(), columnMetadata.build());
    }

    /**
     * List table names, possibly filtered by schema. An empty list is returned if none match.
     *
     * @param session
     * @param schemaNameOrNull
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        return extractClient.getTableNames(schemaNameOrNull);
    }

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @param session
     * @param tableHandle
     * @throws RuntimeException if table handle is no longer valid
     */
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                      ConnectorTableHandle tableHandle) {
        ExtractTableHandle extractTableHandle = (ExtractTableHandle) tableHandle;
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ExtractColumnHandle column : extractClient.getColumns(extractTableHandle)) {
            columnHandles.put(column.getColumnMetadata().getName(), column);
        }
        return columnHandles.build();
    }

    /**
     * Gets the metadata for the specified table column.
     *
     * @param session
     * @param tableHandle
     * @param columnHandle
     * @throws RuntimeException if table or column handles are no longer valid
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle) {
        return ((ExtractColumnHandle) columnHandle).getColumnMetadata();
    }

    /**
     * Gets the metadata for all columns that match the specified table prefix.
     *
     * @param session
     * @param prefix
     */
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix) {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables;
        if (prefix.getTableName() != null) {
            tables = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        } else {
            tables = listTables(session, prefix.getSchemaName());
        }
        for (SchemaTableName tableName : tables) {
            try {
                ExtractTableHandle tableHandle = extractClient.getTableHandle(tableName);
                if (tableHandle == null) {
                    continue;
                }
                columns.put(tableName, getTableMetadata(session, tableHandle).getColumns());
            } catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }
//todo how to impl
//    @Override
//    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
//        logger.info("create table======%s", tableMetadata.toString());
//        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
//        finishCreateTable(session, outputTableHandle, ImmutableList.of());
//    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ExtractTableHandle handle = (ExtractTableHandle) tableHandle;
        extractClient.dropTable(handle);
    }


    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session,
                                                       ConnectorTableMetadata tableMetadata,
                                                       Optional<ConnectorNewTableLayout> layout) {
        ExtractOutputTableHandle handle = extractClient.beginWriteTable(tableMetadata);
        setRollback(() -> extractClient.rollbackCreateTable(handle));
        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
                                                               ConnectorOutputTableHandle tableHandle,
                                                               Collection<Slice> fragments) {
        ExtractOutputTableHandle handle = (ExtractOutputTableHandle) tableHandle;
        extractClient.commitCreateTable(handle);
        clearRollback();
        return Optional.empty();
    }

    private void setRollback(Runnable action) {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    private void clearRollback() {
        rollbackAction.set(null);
    }

    public void rollback() {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }
}
