package org.jpalite.column;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface ColumnProcessor<T> {

    T process(ResultSet rs, int columnIndex) throws SQLException;

}
