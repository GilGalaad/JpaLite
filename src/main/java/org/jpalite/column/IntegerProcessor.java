package org.jpalite.column;

import java.sql.ResultSet;
import java.sql.SQLException;

public class IntegerProcessor implements ColumnProcessor<Integer> {

    @Override
    public Integer process(ResultSet rs, int columnIndex) throws SQLException {
        Object value = rs.getObject(columnIndex);
        return value == null ? null : rs.getInt(columnIndex);
    }

}
