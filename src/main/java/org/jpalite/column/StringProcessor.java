package org.jpalite.column;

import lombok.extern.log4j.Log4j2;

import java.sql.ResultSet;
import java.sql.SQLException;

@Log4j2
public class StringProcessor implements ColumnProcessor<String> {

    @Override
    public String process(ResultSet rs, int columnIndex) throws SQLException {
        return rs.getString(columnIndex);
    }

}
