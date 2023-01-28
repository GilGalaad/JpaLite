package org.jpalite.column;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BigDecimalProcessor implements ColumnProcessor<BigDecimal> {

    @Override
    public BigDecimal process(ResultSet rs, int columnIndex) throws SQLException {
        Object value = rs.getObject(columnIndex);
        return value == null ? null : rs.getBigDecimal(columnIndex);
    }

}
