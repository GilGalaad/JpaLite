package org.jpalite.column;

import lombok.extern.log4j.Log4j2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

@Log4j2
public class DateProcessor implements ColumnProcessor<Date> {

    @Override
    public Date process(ResultSet rs, int columnIndex) throws SQLException {
        Timestamp value = rs.getTimestamp(columnIndex);
        return value == null ? null : new Date(value.getTime());
    }

}
