package org.jpalite.row;

import lombok.extern.log4j.Log4j2;
import org.jpalite.annotation.Column;
import org.jpalite.column.ColumnProcessorFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Log4j2
public class BeanProcessor<T> implements RowProcessor<T> {

    private final Class<T> clazz;
    private final List<ColumnMetaData> cpmds;

    public BeanProcessor(Class<T> clazz, ResultSetMetaData rsmd) throws SQLException {
        this.clazz = clazz;
        this.cpmds = new ArrayList<>(rsmd.getColumnCount());

        List<Field> beanFields = Arrays.asList(clazz.getDeclaredFields());
        BeanInfo beanInfo = getBeanInfo(clazz);
        List<PropertyDescriptor> propertyDescriptors = Arrays.asList(beanInfo.getPropertyDescriptors());

        if (rsmd.getColumnCount() != beanFields.stream().filter(i -> !"__$lineHits$__".equals(i.getName())).count()) {
            throw new SQLException(String.format("ResultSet has %d columns but %s has %d fields", rsmd.getColumnCount(), clazz.getSimpleName(), beanFields.size()));
        }

        for (int i = 0; i < rsmd.getColumnCount(); i++) {
            ColumnMetaData column = new ColumnMetaData();
            column.setColumnIndex(i + 1);

            String columnName = rsmd.getColumnLabel(i + 1);
            column.setColumnName(columnName);

            Field field = beanFields.stream().filter(f -> (f.isAnnotationPresent(Column.class) && columnName.equals(f.getAnnotation(Column.class).name())) || columnName.equals(f.getName())).findFirst().orElse(null);
            if (field == null) {
                throw new SQLException(String.format("No suitable field found in class %s to map column %s", clazz.getSimpleName(), columnName));
            }
            String fieldName = field.getName();
            column.setFieldName(fieldName);
            column.setPrimitive(field.getType().isPrimitive());
            column.setColumnProcessor(ColumnProcessorFactory.create(field.getType()));

            PropertyDescriptor propertyDescriptor = propertyDescriptors.stream().filter(pr -> fieldName.equals(pr.getName())).findFirst().orElse(null);
            if (propertyDescriptor != null && propertyDescriptor.getWriteMethod() != null) {
                column.setWriteMethod(propertyDescriptor.getWriteMethod());
            } else {
                throw new SQLException(String.format("No suitable setter method found for field %s", fieldName));
            }

            cpmds.add(column);
        }
        //cpmds.forEach(log::info);
    }

    @Override
    public T process(ResultSet rs) throws SQLException {
        T ret;
        try {
            ret = clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            throw new SQLException(String.format("No default constructor found in class %s", clazz.getSimpleName()), ex);
        }

        for (var column : cpmds) {
            Object value = column.getColumnProcessor().process(rs, column.getColumnIndex());
            try {
                if (column.isPrimitive() && value == null) {
                    continue;
                }
                column.getWriteMethod().invoke(ret, value);
            } catch (IllegalAccessException | InvocationTargetException ex) {
                throw new SQLException(String.format("Error invoking setter method %s", column.getWriteMethod()));
            }
        }
        return ret;
    }

    private BeanInfo getBeanInfo(Class<T> clazz) throws SQLException {
        try {
            return Introspector.getBeanInfo(clazz);
        } catch (IntrospectionException ex) {
            throw new SQLException("Introspection of " + clazz.getSimpleName() + " class failed", ex);
        }
    }

}