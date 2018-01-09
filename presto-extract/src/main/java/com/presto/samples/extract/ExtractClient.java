package com.presto.samples.extract;


import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.spi.type.Varchars;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.min;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/**
 * Created by ethan
 * 17-10-18
 * <p>
 * 暂时仅具体实现mysql做测试
 */
public class ExtractClient {

    private final Logger logger = Logger.get(ExtractClient.class);

    private final String connectorId;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final Driver driver;
    private final String identifierQuote = "`"; //mysql

    @Inject
    public ExtractClient(ExtractConnectorId connectorId, ExtractConfig config) throws IOException {
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
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("useSSL", "false");
        try {
            driver = DriverManager.getDriver(connectionUrl);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public Set<String> getSchemaNames() {
        logger.info(driver.toString());
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
             ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            logger.info("get schemas from %s", connectionUrl);
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet
                        .getString("TABLE_CAT")
                        .toLowerCase(ENGLISH);
                // skip internal schemas
                if (!schemaName.equals("information_schema") && !schemaName.equals("mysql")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }


    @Nullable
    public ExtractTableHandle getTableHandle(SchemaTableName schemaTableName) {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase(ENGLISH);
                jdbcTableName = jdbcTableName.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName)) {
                List<ExtractTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new ExtractTableHandle(
                            connectorId,
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public List<SchemaTableName> getTableNames(@Nullable String schema) {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers() && (schema != null)) {
                schema = schema.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, schema, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public List<ExtractColumnHandle> getColumns(ExtractTableHandle tableHandle) {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<ExtractColumnHandle> columns = new ArrayList<>();
                boolean found = false;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet.getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"));
                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new ExtractColumnHandle(connectorId, columnName, columnType));
                    }
                }
                if (!found) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED, "Table has no supported column types: " + tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public Connection getConnection(ExtractSplit split)
            throws SQLException {
        Connection connection = driver.connect(split.getConnectionUrl(), split.getConnectionProperties());
        try {
            connection.setReadOnly(true);
        } catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    public PreparedStatement buildSql(Connection connection, ExtractSplit split,
                                      List<ExtractColumnHandle> columnHandles)
            throws SQLException {
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain());
    }

    @SuppressWarnings("unchecked")
    public ExtractOutputTableHandle beginWriteTable(ConnectorTableMetadata tableMetadata) {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();
        if (!getSchemaNames().contains(schema)) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schema);
        }
        String connectUrl = connectionUrl;
        Properties connectProperties = connectionProperties;
        Map<String, Object> map = tableMetadata.getProperties();
        if (!isNull(map) && !map.isEmpty()) {
            if (map.containsKey(ExtractTableProperties.LOCATION_PROPERTY)) {
                connectUrl = String.valueOf(map.get(ExtractTableProperties.LOCATION_PROPERTY));
            }
            if (map.containsKey(ExtractTableProperties.CUSTOM_MAP)) {
                try {
                    //todo user="",password="",useUnicode="",characterEncoding="",useSSL=""参数要和mysql一致
                    Map<String, String> customProperties = (Map<String, String>) new ObjectMapper()
                            .readValue(String.valueOf(map.get(ExtractTableProperties.CUSTOM_MAP)),
                                    Map.class);
                    Properties properties = new Properties();
                    customProperties.forEach(properties::setProperty);
                    connectProperties = properties;
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        try (Connection connection = driver.connect(connectUrl, connectProperties)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                table = table.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            String temporaryName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
            StringBuilder sql = new StringBuilder()
                    .append("CREATE TABLE ")
                    .append(quoted(catalog, schema, temporaryName))
                    .append(" (");
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(new StringBuilder()
                        .append(quoted(columnName))
                        .append(" ")
                        .append(toSqlType_mysql(column.getType()))
                        .toString());
            }
            Joiner.on(", ").appendTo(sql, columnList.build());
            sql.append(")");

            execute(connection, sql.toString());

            return new ExtractOutputTableHandle(
                    connectorId,
                    catalog,
                    schema,
                    table,
                    columnNames.build(),
                    columnTypes.build(),
                    temporaryName,
                    connectUrl,
                    connectProperties);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public void commitCreateTable(ExtractOutputTableHandle handle) {
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" RENAME TO ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));
        try (Connection connection = driver.connect(handle.getConnectionUrl(), handle.getConnectionProperties())) {
            execute(connection, sql.toString());
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public void dropTable(ExtractTableHandle handle) {
        StringBuilder sql = new StringBuilder()
                .append("DROP TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            execute(connection, sql.toString());
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public void rollbackCreateTable(ExtractOutputTableHandle handle) {
        StringBuilder sql = new StringBuilder()
                .append("DROP TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()));
        try (Connection connection = driver.connect(handle.getConnectionUrl(), handle.getConnectionProperties())) {
            execute(connection, sql.toString());
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private void execute(Connection connection, String query)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            logger.info("Execute sql: %s", query);
            statement.execute(query);
        }
    }

    private String toSqlType_base(Type type) {
        if (type instanceof VarcharType) {
            if (((VarcharType) type).isUnbounded()) {
                return "varchar";
            }
            return "varchar(" + ((VarcharType) type).getLength() + ")";
        }
        if (type instanceof CharType) {
            if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
                return "char";
            }
            return "char(" + ((CharType) type).getLength() + ")";
        }

        String sqlType = SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getTypeSignature());
    }

    /**
     * mysql
     */
    private String toSqlType_mysql(Type type) {
        if (Varchars.isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.getLength() <= 255) {
                return "tinytext";
            }
            if (varcharType.getLength() <= 65535) {
                return "text";
            }
            if (varcharType.getLength() <= 16777215) {
                return "mediumtext";
            }
            return "longtext";
        }

        String sqlType = toSqlType_base(type);
        switch (sqlType) {
            case "varbinary":
                return "mediumblob";
            case "time with timezone":
                return "time";
            case "timestamp":
            case "timestamp with timezone":
                return "datetime";
        }
        return sqlType;
    }

    private Type toPrestoType(int jdbcType, int columnSize) {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
                return TINYINT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.REAL:
                return REAL;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
                return createCharType(min(columnSize, CharType.MAX_LENGTH));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return createUnboundedVarcharType();
                }
                return createVarcharType(columnSize);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VARBINARY;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
        }
        return null;
    }

    private ResultSet getColumns(ExtractTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }

    private ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException {
        // MySQL maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                schemaName,
                null,
                escapeNamePattern(tableName, escape),
                new String[]{"TABLE", "VIEW"});
    }

    private String escapeNamePattern(String name, String escape) {
        if ((name == null) || (escape == null)) {
            return name;
        }
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        return name;
    }

    private SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException {
        // MySQL uses catalogs instead of schemas
        return new SchemaTableName(
                resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }

    private String quoted(String name) {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    private String quoted(String catalog, String schema, String table) {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    private static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "boolean")
            .put(BIGINT, "bigint")
            .put(INTEGER, "integer")
            .put(SMALLINT, "smallint")
            .put(TINYINT, "tinyint")
            .put(DOUBLE, "double precision")
            .put(REAL, "real")
            .put(VARBINARY, "varbinary")
            .put(DATE, "date")
            .put(TIME, "time")
            .put(TIME_WITH_TIME_ZONE, "time with timezone")
            .put(TIMESTAMP, "timestamp")
            .put(TIMESTAMP_WITH_TIME_ZONE, "timestamp with timezone")
            .build();
}
