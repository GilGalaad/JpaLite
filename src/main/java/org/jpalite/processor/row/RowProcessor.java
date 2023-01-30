package org.jpalite.processor.row;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface RowProcessor<T> {

    T process(ResultSet rs) throws SQLException;

}
