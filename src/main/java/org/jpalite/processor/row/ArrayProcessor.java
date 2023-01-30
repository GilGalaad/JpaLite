package org.jpalite.processor.row;

import org.jpalite.dto.ColumnMapping;
import org.jpalite.processor.column.ColumnProcessorFactory;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ArrayProcessor<T> implements RowProcessor<T> {

    private final Class<?> componentClass;
    private final List<ColumnMapping> columnMappings;

    public ArrayProcessor(Class<T> clazz, ResultSetMetaData resultSetMetaData) throws SQLException {
        this.componentClass = clazz.getComponentType();
        this.columnMappings = new ArrayList<>(resultSetMetaData.getColumnCount());

        for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
            ColumnMapping columnMapping = new ColumnMapping();
            columnMapping.setColumnIndex(i + 1);
            columnMapping.setColumnLabel(resultSetMetaData.getColumnLabel(i + 1));
            columnMapping.setPrimitive(componentClass.isPrimitive());
            columnMapping.setColumnProcessor(ColumnProcessorFactory.create(componentClass));
            columnMappings.add(columnMapping);
        }

    }

    @Override
    public T process(ResultSet rs) throws SQLException {
        @SuppressWarnings("unchecked")
        T ret = (T) Array.newInstance(componentClass, columnMappings.size());
        for (var columnMapping : columnMappings) {
            Object value = columnMapping.getColumnProcessor().process(rs, columnMapping.getColumnIndex());
            if (columnMapping.isPrimitive() && value == null) {
                throw new SQLException(String.format("Cannot assign null value to a primitive type for column %s", columnMapping.getColumnLabel()));
            }
            Array.set(ret, columnMapping.getColumnIndex() - 1, value);
        }
        return ret;
    }

}
