package org.jpalite.column;

import lombok.extern.log4j.Log4j2;

import java.sql.ResultSet;
import java.sql.SQLException;

@Log4j2
public class IntegerProcessor implements ColumnProcessor<Integer> {

    @Override
    public Integer process(ResultSet rs, int columnIndex) throws SQLException {
        Object value = rs.getObject(columnIndex);
        return value == null ? null : rs.getInt(columnIndex);
    }

}
