package org.jpalite.column;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DoubleProcessor implements ColumnProcessor<Double> {

    @Override
    public Double process(ResultSet rs, int columnIndex) throws SQLException {
        Object value = rs.getObject(columnIndex);
        return value == null ? null : rs.getDouble(columnIndex);
    }

}
