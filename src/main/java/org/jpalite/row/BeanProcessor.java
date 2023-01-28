package org.jpalite.row;

import org.jpalite.column.ColumnProcessorFactory;
import org.jpalite.dto.ColumnMetaData;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.jpalite.ReflectionUtils.*;

public class BeanProcessor<T> implements RowProcessor<T> {

    private final Class<T> clazz;
    private final List<ColumnMetaData> columnMetaData;

    public BeanProcessor(Class<T> clazz, ResultSetMetaData resultSetMetaData) throws SQLException {
        this.clazz = clazz;
        this.columnMetaData = new ArrayList<>(resultSetMetaData.getColumnCount());

        List<Field> beanFields = getBeanFields(clazz);
        List<PropertyDescriptor> propertyDescriptors = getPropertyDescriptorsForFields(clazz);
        if (resultSetMetaData.getColumnCount() != beanFields.size()) {
            throw new SQLException(String.format("ResultSet has %d columns but %s has %d fields", resultSetMetaData.getColumnCount(), clazz.getSimpleName(), beanFields.size()));
        }

        for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
            ColumnMetaData column = new ColumnMetaData();
            column.setColumnIndex(i + 1);

            String columnName = resultSetMetaData.getColumnLabel(i + 1);
            column.setColumnName(columnName);

            Field field = getFieldForColumn(beanFields, columnName);
            String fieldName = field.getName();
            column.setFieldName(fieldName);
            column.setPrimitive(field.getType().isPrimitive());
            column.setColumnProcessor(ColumnProcessorFactory.create(field.getType()));

            PropertyDescriptor propertyDescriptor = getPropertyDescriptorForField(propertyDescriptors, field);
            if (propertyDescriptor.getWriteMethod() != null) {
                column.setWriteMethod(propertyDescriptor.getWriteMethod());
            } else {
                throw new SQLException(String.format("No suitable setter method found for field %s of class %s", fieldName, clazz.getSimpleName()));
            }

            columnMetaData.add(column);
        }
    }

    @Override
    public T process(ResultSet rs) throws SQLException {
        T ret;
        try {
            ret = clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            throw new SQLException(String.format("No default constructor found in class %s", clazz.getSimpleName()), ex);
        }

        for (var cmd : columnMetaData) {
            Object value = cmd.getColumnProcessor().process(rs, cmd.getColumnIndex());
            if (cmd.isPrimitive() && value == null) {
                throw new SQLException(String.format("Cannot assign null value to a primitive type for column %s", cmd.getColumnName()));
            }
            invokeWrapper(cmd.getWriteMethod(), ret, value);
        }
        return ret;
    }

}
