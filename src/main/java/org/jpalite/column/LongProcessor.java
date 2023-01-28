package org.jpalite.column;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LongProcessor implements ColumnProcessor<Long> {

    @Override
    public Long process(ResultSet rs, int columnIndex) throws SQLException {
        Object value = rs.getObject(columnIndex);
        return value == null ? null : rs.getLong(columnIndex);
    }

}
