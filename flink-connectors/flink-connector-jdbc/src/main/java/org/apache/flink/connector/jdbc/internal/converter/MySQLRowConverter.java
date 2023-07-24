/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * MySQL.
 */
public class MySQLRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "MySQL";
    }

    public MySQLRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        int precision;
        int scale;
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case INTEGER:
                return val -> val;
            case BIGINT:
                return val -> val;
            case DECIMAL:
                precision = ((DecimalType) type).getPrecision();
                scale = ((DecimalType) type).getScale();
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> (int) (((Date) val).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val -> (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val ->
                        val instanceof LocalDateTime
                                ? TimestampData.fromLocalDateTime((LocalDateTime) val)
                                : TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> {
                    if (val instanceof byte[]) {
                        byte[] tem = (byte[]) val;
                        return StringData.fromString(new String(tem));
                    }
                    return StringData.fromString(val.toString());
                };
            case BINARY:
            case VARBINARY:
                return val -> val;
        }
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
}
