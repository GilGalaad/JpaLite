package org.jpalite.processor.row;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class RowProcessorFactory {

    public static <T> RowProcessor<T> create(Class<T> clazz, ResultSetMetaData resultSetMetaData) throws SQLException {
        if (resultSetMetaData.getColumnCount() == 1) {
            return new ScalarProcessor<>(clazz, resultSetMetaData);
        }
        return new BeanProcessor<>(clazz, resultSetMetaData);
    }

}
