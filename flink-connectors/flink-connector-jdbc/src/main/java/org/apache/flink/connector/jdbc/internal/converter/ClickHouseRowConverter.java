package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

/**
 * @Author: Franksen @Program: org.apache.flink.connector.jdbc.dialect @Date: 2020/11/29
 * 22:49 @Description:
 */
public class ClickHouseRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    public ClickHouseRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "ClickHouse";
    }

}
