package org.apache.flink.connector.jdbc.util;

/**
 * @Author: Franksen @Program: org.apache.flink.connector.jdbc.util @Date: 2023/7/24
 * 14:32 @Description:
 */
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.mysql.cj.MysqlType;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataTypeUtils {

    public static RowType appendRowFields(RowType rowType, List<RowType.RowField> fields) {
        List<RowType.RowField> newFields =
                Stream.concat(rowType.getFields().stream(), fields.stream())
                        .collect(Collectors.toList());
        return new RowType(rowType.isNullable(), newFields);
    }

    public static String getFlinkType(LogicalTypeRoot type, int precision, int scale) {
        switch (type) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TIMESTAMP(6)";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case CHAR:
            case VARCHAR:
                return "STRING";
            case BINARY:
            case VARBINARY:
                return "BINARY";
            case DECIMAL:
                return String.format("DECIMAL(%d,%d)", precision, scale);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    public static DataType fromMysqlType(String dataType, int dataLength, int dataScale)
            throws SQLException {
        MysqlType mysqlType = MysqlType.getByName(dataType);
        switch (mysqlType) {
            case TINYINT:
            case TINYINT_UNSIGNED:
                return DataTypes.TINYINT();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case SMALLINT:
            case SMALLINT_UNSIGNED:
                return DataTypes.SMALLINT();
            case INT:
            case INT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                return DataTypes.INT();
            case BIGINT:
            case BIGINT_UNSIGNED:
                return DataTypes.BIGINT();
            case BLOB:
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
            case BINARY:
            case VARBINARY:
                return DataTypes.BYTES();
            case CHAR:
            case VARCHAR:
            case TEXT:
            case LONGTEXT:
            case MEDIUMTEXT:
            case TINYTEXT:
            case JSON:
                return DataTypes.STRING();
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return DataTypes.DOUBLE();
            case FLOAT:
            case FLOAT_UNSIGNED:
                return DataTypes.FLOAT();
            case TIME:
                return DataTypes.TIME();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
            case TIMESTAMP:
                return DataTypes.TIMESTAMP(3);
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                return DataTypes.DECIMAL(dataLength, dataScale);
        }
        throw new UnsupportedOperationException(
                String.format("Doesn't support mysql type '%s' yet", dataType));
    }
}
