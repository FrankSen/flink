package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @Author: Franksen @Program: org.apache.flink.connector.jdbc.catalog @Date: 2023/7/11
 * 9:01 @Description:
 */
public class MySQLCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLCatalog.class);

    private static final Set<String> builtinDatabases = new HashSet<String>() {};

    Map<String, List<String>> tablesMap;

    public static final String MYSQL_BIT = "BIT";

    public static final String MYSQL_TINYINT = "TINYINT";

    public static final String MYSQL_SMALLINT = "SMALLINT";

    public static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";

    public static final String MYSQL_INT = "INT";

    public static final String MYSQL_MEDIUMINT = "MEDIUMINT";

    public static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";

    public static final String MYSQL_BIGINT = "BIGINT";

    public static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";

    public static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";

    public static final String MYSQL_FLOAT = "FLOAT";

    public static final String MYSQL_DOUBLE = "DOUBLE";

    public static final String MYSQL_NUMERIC = "NUMERIC";

    public static final String MYSQL_DECIMAL = "DECIMAL";

    public static final String MYSQL_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";

    public static final String MYSQL_BOOLEAN = "BOOLEAN";

    public static final String MYSQL_DATE = "DATE";

    public static final String MYSQL_TIME = "TIME";

    public static final String MYSQL_DATETIME = "DATETIME";

    public static final String MYSQL_CHAR = "CHAR";

    public static final String MYSQL_VARCHAR = "VARCHAR";

    public static final String MYSQL_TEXT = "TEXT";

    public static final String MYSQL_TINYTEXT = "TINYTEXT";

    public static final String MYSQL_LONGTEXT = "LONGTEXT";

    public static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";

    public static final String MYSQL_JSON = "JSON";

    public static final String MYSQL_TIMESTAMP = "TIMESTAMP";

    public static final String MYSQL_VARBINARY = "VARBINARY";

    public static final String MYSQL_BINARY = "BINARY";

    public static final String MYSQL_BLOB = "BLOB";

    public static final String MYSQL_TINYBLOB = "TINYBLOB";

    public static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";

    public static final String MYSQL_LONGBLOB = "LONGBLOB";

    public MySQLCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
        this.tablesMap = new HashMap<>();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> mysqlDatabases = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(baseUrl, username, pwd)) {
            PreparedStatement statement =
                    conn.prepareStatement("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String dbName = resultSet.getString(1);
                if (!builtinDatabases.contains(dbName)) mysqlDatabases.add(resultSet.getString(1));
            }
            return mysqlDatabases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName))
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        throw new DatabaseNotExistException(getName(), databaseName);
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName))
            throw new DatabaseNotExistException(getName(), databaseName);
        List<String> listTables = tablesMap.get(databaseName);
        if (listTables != null) return listTables;
        try (Connection conn = DriverManager.getConnection(baseUrl, username, pwd)) {
            PreparedStatement preparedStatement =
                    conn.prepareStatement(
                            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.`tables` where `table_schema` = ?;");
            preparedStatement.setString(1, databaseName);
            ResultSet rs = preparedStatement.executeQuery();
            listTables = new ArrayList<>();
            while (rs.next()) {
                listTables.add(rs.getString(1));
                LOG.info(
                        "MySQLCatalog db: {}, schema: {} has tables: {}",
                        databaseName,
                        getSchema(),
                        listTables);
                tablesMap.put(databaseName, listTables);
            }
            return listTables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) throw new TableNotExistException(getName(), tablePath);
        String dbUrl = baseUrl + tablePath.getDatabaseName();
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey =
                    getPrimaryKey(metaData, null, tablePath.getObjectName());
            PreparedStatement preparedStatement =
                    conn.prepareStatement(
                            String.format("SELECT * FROM %s limit 1", tablePath.getObjectName()));
            ResultSet rs = preparedStatement.executeQuery();
            ResultSetMetaData rsMetaData = rs.getMetaData();
            String[] names = new String[rsMetaData.getColumnCount()];
            DataType[] dataTypes = new DataType[rsMetaData.getColumnCount()];
            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                names[i - 1] = rsMetaData.getColumnName(i);
                dataTypes[i - 1] = fromJdbcType(rsMetaData, i);
                if (rsMetaData.isNullable(i) == 0) dataTypes[i - 1] = dataTypes[i - 1].notNull();
            }
            LOG.info("table names: {}, dataTypes: {}", names, dataTypes);
            TableSchema.Builder tableBuilder = (new TableSchema.Builder()).fields(names, dataTypes);
            primaryKey.ifPresent(
                    pk ->
                            tableBuilder.primaryKey(
                                    pk.getName(), pk.getColumns().toArray(new String[0])));
            TableSchema tableSchema = tableBuilder.build();
            HashMap<String, String> props = new HashMap<>();
            props.put(FactoryUtil.CONNECTOR.key(), "jdbc");
            props.put(JdbcConnectorOptions.URL.key(), dbUrl);
            props.put(JdbcConnectorOptions.TABLE_NAME.key(), tablePath.getObjectName());
            props.put(JdbcConnectorOptions.USERNAME.key(), this.username);
            props.put(JdbcConnectorOptions.PASSWORD.key(), this.pwd);
            return new CatalogTableImpl(tableSchema, props, "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private DataType fromJdbcType(ResultSetMetaData rsMetaData, int i) throws SQLException {
        String mysqlColumnType = rsMetaData.getColumnTypeName(i);
        int type = rsMetaData.getColumnType(i);
        int precision = rsMetaData.getPrecision(i);
        int scale = rsMetaData.getScale(i);
        switch (mysqlColumnType) {
            case MYSQL_TINYINT:
                if (1 == precision) return DataTypes.BOOLEAN();
                return DataTypes.TINYINT();
            case MYSQL_BIT:
                if (1 == precision) return DataTypes.BOOLEAN();
                return DataTypes.BYTES();
            case MYSQL_SMALLINT:
                return DataTypes.SMALLINT();
            case MYSQL_TINYINT_UNSIGNED:
                return DataTypes.SMALLINT();
            case MYSQL_INT:
                return DataTypes.INT();
            case MYSQL_MEDIUMINT:
                return DataTypes.INT();
            case MYSQL_SMALLINT_UNSIGNED:
                return DataTypes.INT();
            case MYSQL_BIGINT:
                return DataTypes.BIGINT();
            case MYSQL_INT_UNSIGNED:
                return DataTypes.BIGINT();
            case MYSQL_BIGINT_UNSIGNED:
                return DataTypes.DECIMAL(20, 0);
            case MYSQL_FLOAT:
                return DataTypes.FLOAT();
            case MYSQL_DOUBLE:
                return DataTypes.DOUBLE();
            case MYSQL_NUMERIC:
            case MYSQL_DECIMAL:
            case MYSQL_DECIMAL_UNSIGNED:
                if (precision <= 38) return DataTypes.DECIMAL(precision, scale);
                return DataTypes.STRING();
            case MYSQL_BOOLEAN:
                return DataTypes.BOOLEAN();
            case MYSQL_DATE:
                return DataTypes.DATE();
            case MYSQL_TIME:
                return DataTypes.TIME(scale);
            case MYSQL_DATETIME:
                return DataTypes.TIMESTAMP(scale);
            case MYSQL_CHAR:
                return DataTypes.CHAR(precision);
            case MYSQL_VARCHAR:
                return DataTypes.CHAR(precision);
            case MYSQL_TEXT:
            case MYSQL_TINYTEXT:
            case MYSQL_MEDIUMTEXT:
            case MYSQL_LONGTEXT:
            case MYSQL_JSON:
            case MYSQL_TINYBLOB:
            case MYSQL_BLOB:
            case MYSQL_MEDIUMBLOB:
            case MYSQL_LONGBLOB:
                return DataTypes.STRING();
            case MYSQL_TIMESTAMP:
                return DataTypes.TIMESTAMP(3);
            case MYSQL_VARBINARY:
            case MYSQL_BINARY:
                return DataTypes.BYTES();
        }
        throw new UnsupportedOperationException(
                String.format("Doesn't support mysql type '%s' yet", mysqlColumnType));
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List<String> tables = null;
        try {
            tables = listTables(tablePath.getDatabaseName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
        return tables.contains(tablePath.getObjectName());
    }
}
