package org.jpalite.column;

import java.sql.SQLException;

public class ColumnProcessorFactory {

    public static <T> ColumnProcessor<?> create(Class<T> clazz) throws SQLException {
        return switch (clazz.getName()) {
            case "java.lang.String" -> new StringProcessor();
            case "short", "java.lang.Short" -> new ShortProcessor();
            case "int", "java.lang.Integer" -> new IntegerProcessor();
            case "long", "java.lang.Long" -> new LongProcessor();
            case "float", "java.lang.Float" -> new FloatProcessor();
            case "double", "java.lang.Double" -> new DoubleProcessor();
            case "java.math.BigDecimal" -> new BigDecimalProcessor();
            case "java.util.Date" -> new DateProcessor();
            case "java.lang.Object" -> new ObjectProcessor();
            default -> throw new SQLException(String.format("Unsupported column processor for class %s", clazz.getSimpleName()));
        };
    }

}
