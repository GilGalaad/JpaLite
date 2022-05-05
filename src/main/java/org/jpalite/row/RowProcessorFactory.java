package org.jpalite.row;

import lombok.extern.log4j.Log4j2;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Log4j2
public class RowProcessorFactory {

    public static <T> RowProcessor<T> create(Class<T> clazz, ResultSetMetaData resultSetMetaData) throws SQLException {
        if (resultSetMetaData.getColumnCount() == 1) {
            return new ScalarProcessor<>(clazz, resultSetMetaData);
        }
        if (clazz.isArray()) {
            return new ArrayProcessor<>(clazz, resultSetMetaData);
        }
        return new BeanProcessor<>(clazz, resultSetMetaData);
    }

}
