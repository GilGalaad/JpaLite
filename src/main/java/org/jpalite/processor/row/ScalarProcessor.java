package org.jpalite.processor.row;

import org.jpalite.dto.ColumnMapping;
import org.jpalite.processor.column.ColumnProcessorFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ScalarProcessor<T> implements RowProcessor<T> {

    private final Class<T> clazz;
    private final ColumnMapping columnMapping;

    public ScalarProcessor(Class<T> clazz, ResultSetMetaData resultSetMetaData) throws SQLException {
        this.clazz = clazz;
        this.columnMapping = new ColumnMapping();
        columnMapping.setColumnIndex(1);
        columnMapping.setColumnLabel(resultSetMetaData.getColumnLabel(1));
        columnMapping.setPrimitive(clazz.isPrimitive());
        columnMapping.setColumnProcessor(ColumnProcessorFactory.create(clazz));
    }

    @Override
    public T process(ResultSet rs) throws SQLException {
        @SuppressWarnings("unchecked")
        T value = (T) columnMapping.getColumnProcessor().process(rs, 1);
        if (columnMapping.isPrimitive() && value == null) {
            throw new SQLException(String.format("Cannot assign null value to a primitive type for column %s", columnMapping.getColumnLabel()));
        }
        return value;
    }

}
