package org.jpalite.processor.column;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class DateProcessor implements ColumnProcessor<Date> {

    @Override
    public Date process(ResultSet rs, int columnIndex) throws SQLException {
        Timestamp value = rs.getTimestamp(columnIndex);
        return value == null ? null : new Date(value.getTime());
    }

}
