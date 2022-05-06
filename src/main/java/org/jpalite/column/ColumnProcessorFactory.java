package org.jpalite.column;

import lombok.extern.log4j.Log4j2;

import java.sql.SQLException;

@Log4j2
public class ColumnProcessorFactory {

    public static <T> ColumnProcessor<?> create(Class<T> clazz) throws SQLException {
        switch (clazz.getName()) {
            case "java.lang.String":
                return new StringProcessor();
            case "short":
            case "java.lang.Short":
                return new ShortProcessor();
            case "int":
            case "java.lang.Integer":
                return new IntegerProcessor();
            case "long":
            case "java.lang.Long":
                return new LongProcessor();
            case "java.util.Date":
                return new DateProcessor();
            case "java.lang.Object":
                return new ObjectProcessor();
            default:
                throw new SQLException(String.format("Unsupported column processor for class %s", clazz.getSimpleName()));
        }
    }

}
