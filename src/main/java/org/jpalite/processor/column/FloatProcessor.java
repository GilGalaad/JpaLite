package org.jpalite.processor.column;

import java.sql.ResultSet;
import java.sql.SQLException;

public class FloatProcessor implements ColumnProcessor<Float> {

    @Override
    public Float process(ResultSet rs, int columnIndex) throws SQLException {
        Object value = rs.getObject(columnIndex);
        return value == null ? null : rs.getFloat(columnIndex);
    }

}
