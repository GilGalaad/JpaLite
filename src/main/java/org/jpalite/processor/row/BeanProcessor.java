package org.jpalite.processor.row;

import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;
import org.jpalite.common.StatementUtils;
import org.jpalite.dto.ColumnMapping;
import org.jpalite.processor.column.ColumnProcessorFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class BeanProcessor<T> implements RowProcessor<T> {

    private final Class<T> clazz;
    private final List<ColumnMapping> columnMappings;
    private final List<ColumnMapping> idColumnsMappings;
    private final List<ColumnMapping> dataColumnsMappings;

    public BeanProcessor(Class<T> clazz, ResultSetMetaData resultSetMetaData) throws SQLException {
        this.clazz = clazz;

        if (resultSetMetaData == null && !clazz.isAnnotationPresent(Table.class)) {
            throw new SQLException(String.format("Entity class %s must be @Table annotated", clazz.getSimpleName()));
        }

        // Field is used to get annotations via reflection
        List<Field> beanFields = getBeanFields(clazz);
        // PropertyDescriptor is used to get references to accessor methods
        List<PropertyDescriptor> propertyDescriptors = getBeanPropertyDescriptors(clazz);

        // if we have a result set, we are processing a select query, and resultSetMetaData leads the mapping
        if (resultSetMetaData != null) {
            this.columnMappings = new ArrayList<>(resultSetMetaData.getColumnCount());

            if (resultSetMetaData.getColumnCount() > beanFields.size()) {
                throw new SQLException(String.format("ResultSet has %d columns but class %s has %d fields", resultSetMetaData.getColumnCount(), clazz.getSimpleName(), beanFields.size()));
            }

            for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                ColumnMapping columnMapping = new ColumnMapping();
                columnMapping.setColumnIndex(i + 1);
                columnMapping.setColumnLabel(resultSetMetaData.getColumnLabel(i + 1));

                Field field = getFieldForColumn(beanFields, columnMapping.getColumnLabel());
                columnMapping.setFieldName(field.getName());
                columnMapping.setPrimitive(field.getType().isPrimitive());
                columnMapping.setColumnProcessor(ColumnProcessorFactory.create(field.getType()));

                PropertyDescriptor propertyDescriptor = getPropertyDescriptorForField(propertyDescriptors, field);
                columnMapping.setWriteMethod(propertyDescriptor.getWriteMethod());

                columnMappings.add(columnMapping);
            }
            idColumnsMappings = null;
            dataColumnsMappings = null;
        }
        // else we are inserting or updating, and bean leads the mapping
        else {
            this.columnMappings = new ArrayList<>(beanFields.size());

            for (int i = 0; i < beanFields.size(); i++) {
                ColumnMapping columnMapping = new ColumnMapping();
                columnMapping.setColumnIndex(i + 1);

                Field field = beanFields.get(i);
                columnMapping.setColumnLabel(field.getAnnotation(Column.class).name());
                columnMapping.setFieldName(field.getName());
                columnMapping.setId(field.isAnnotationPresent(Id.class));

                PropertyDescriptor propertyDescriptor = getPropertyDescriptorForField(propertyDescriptors, field);
                columnMapping.setReadMethod(propertyDescriptor.getReadMethod());

                columnMappings.add(columnMapping);
            }
            idColumnsMappings = columnMappings.stream().filter(ColumnMapping::isId).toList();
            dataColumnsMappings = columnMappings.stream().filter(i -> !i.isId()).toList();
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

        for (var columnMapping : columnMappings) {
            Object value = columnMapping.getColumnProcessor().process(rs, columnMapping.getColumnIndex());
            if (columnMapping.isPrimitive() && value == null) {
                throw new SQLException(String.format("Cannot assign null value to a primitive type for column %s", columnMapping.getColumnLabel()));
            }
            invokeWrapper(columnMapping.getWriteMethod(), ret, value);
        }
        return ret;
    }

    public String generateInsertStatement() {
        return "INSERT INTO " + clazz.getAnnotation(Table.class).name()
                + " ("
                + columnMappings.stream().map(ColumnMapping::getColumnLabel).collect(Collectors.joining(", "))
                + ") VALUES ("
                + String.join(", ", Collections.nCopies(columnMappings.size(), "?"))
                + ")";
    }

    public void setInsertParameters(PreparedStatement stmt, ParameterMetaData parameterMetaData, Object obj) throws SQLException {
        List<Object> params = new ArrayList<>(columnMappings.size());
        for (var columnMapping : columnMappings) {
            params.add(invokeWrapper(columnMapping.getReadMethod(), obj));
        }
        StatementUtils.setStatementParameters(stmt, parameterMetaData, params.toArray());
    }

    public String generateUpdateStatement() throws SQLException {
        if (idColumnsMappings.isEmpty()) {
            throw new SQLException(String.format("Entity class %s has no @Id annotated fields", clazz.getSimpleName()));
        }
        if (dataColumnsMappings.isEmpty()) {
            throw new SQLException(String.format("Entity class %s has only @Id annotated fields", clazz.getSimpleName()));
        }
        return "UPDATE "
                + clazz.getAnnotation(Table.class).name()
                + " SET "
                + dataColumnsMappings.stream().map(i -> i.getColumnLabel() + " = ?").collect(Collectors.joining(", "))
                + " WHERE "
                + idColumnsMappings.stream().map(i -> i.getColumnLabel() + " = ?").collect(Collectors.joining(" AND "));
    }

    public void setUpdateParameters(PreparedStatement stmt, ParameterMetaData parameterMetaData, Object obj) throws SQLException {
        List<Object> params = new ArrayList<>(columnMappings.size());
        for (var cmd : dataColumnsMappings) {
            params.add(invokeWrapper(cmd.getReadMethod(), obj));
        }
        for (var cmd : idColumnsMappings) {
            params.add(invokeWrapper(cmd.getReadMethod(), obj));
        }
        StatementUtils.setStatementParameters(stmt, parameterMetaData, params.toArray());
    }

    // reflection/introspection utility methods
    private List<Field> getBeanFields(Class<T> clazz) throws SQLException {
        List<Field> ret = Arrays.stream(clazz.getDeclaredFields()).filter(f -> f.isAnnotationPresent(Column.class)).collect(Collectors.toList());
        if (ret.isEmpty()) {
            throw new SQLException(String.format("Entity class %s has no @Column annotated fields", clazz.getSimpleName()));
        }
        return ret;
    }

    private List<PropertyDescriptor> getBeanPropertyDescriptors(Class<T> clazz) throws SQLException {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
            return Arrays.asList(beanInfo.getPropertyDescriptors());
        } catch (IntrospectionException ex) {
            throw new SQLException("Introspection of " + clazz.getSimpleName() + " class failed", ex);
        }
    }

    private Field getFieldForColumn(List<Field> beanFields, String columnLabel) throws SQLException {
        Field ret = beanFields.stream().filter(f -> f.getAnnotation(Column.class).name().equals(columnLabel)).findFirst().orElse(null);
        if (ret == null) {
            throw new SQLException(String.format("No suitable field found in class %s to map column %s", beanFields.get(0).getDeclaringClass().getSimpleName(), columnLabel));
        }
        return ret;
    }

    private PropertyDescriptor getPropertyDescriptorForField(List<PropertyDescriptor> propertyDescriptors, Field field) throws SQLException {
        PropertyDescriptor ret = propertyDescriptors.stream().filter(pr -> pr.getName().equals(field.getName())).findFirst().orElse(null);
        if (ret == null || ret.getReadMethod() == null || ret.getWriteMethod() == null) {
            throw new SQLException(String.format("No suitable accessor methods found for field %s of class %s", field.getName(), field.getDeclaringClass().getSimpleName()));
        }
        return ret;
    }

    private Object invokeWrapper(Method method, Object obj, Object... args) throws SQLException {
        try {
            return method.invoke(obj, args);
        } catch (IllegalAccessException | InvocationTargetException ex) {
            throw new SQLException(String.format("Reflection error invoking method %s", method.toGenericString()), ex);
        }
    }

}
