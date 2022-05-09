package org.jpalite.row;

import lombok.extern.log4j.Log4j2;
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

@Log4j2
public class BeanProcessor<T> implements RowProcessor<T> {

    private final Class<T> clazz;
    private final List<ColumnMetaData> cmds;

    public BeanProcessor(Class<T> clazz, ResultSetMetaData rsmd) throws SQLException {
        this.clazz = clazz;
        this.cmds = new ArrayList<>(rsmd.getColumnCount());

        List<Field> beanFields = getBeanFields(clazz);
        List<PropertyDescriptor> propertyDescriptors = getPropertyDescriptorsForFields(clazz);
        if (rsmd.getColumnCount() != beanFields.size()) {
            throw new SQLException(String.format("ResultSet has %d columns but %s has %d fields", rsmd.getColumnCount(), clazz.getSimpleName(), beanFields.size()));
        }

        for (int i = 0; i < rsmd.getColumnCount(); i++) {
            ColumnMetaData column = new ColumnMetaData();
            column.setColumnIndex(i + 1);

            String columnName = rsmd.getColumnLabel(i + 1);
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

            cmds.add(column);
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

        for (var cmd : cmds) {
            Object value = cmd.getColumnProcessor().process(rs, cmd.getColumnIndex());
            try {
                if (cmd.isPrimitive() && value == null) {
                    throw new SQLException(String.format("Cannot assign null value to a primitive type for column %s", cmd.getColumnName()));
                }
                cmd.getWriteMethod().invoke(ret, value);
            } catch (IllegalAccessException | InvocationTargetException ex) {
                throw new SQLException(String.format("Error invoking setter method %s", cmd.getWriteMethod()));
            }
        }
        return ret;
    }

}
