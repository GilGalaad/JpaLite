package org.jpalite.column;

import lombok.extern.log4j.Log4j2;

import java.sql.ResultSet;
import java.sql.SQLException;

@Log4j2
public class ObjectProcessor implements ColumnProcessor<Object> {

    @Override
    public Object process(ResultSet rs, int columnIndex) throws SQLException {
        return rs.getObject(columnIndex);
    }

}
