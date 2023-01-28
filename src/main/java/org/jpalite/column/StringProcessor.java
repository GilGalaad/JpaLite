package org.jpalite.column;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StringProcessor implements ColumnProcessor<String> {

    @Override
    public String process(ResultSet rs, int columnIndex) throws SQLException {
        return rs.getString(columnIndex);
    }

}
