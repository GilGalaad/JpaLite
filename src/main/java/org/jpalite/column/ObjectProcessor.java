package org.jpalite.column;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ObjectProcessor implements ColumnProcessor<Object> {

    @Override
    public Object process(ResultSet rs, int columnIndex) throws SQLException {
        return rs.getObject(columnIndex);
    }

}
