package org.jpalite.row;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface RowProcessor<T> {

    T process(ResultSet rs) throws SQLException;

}
