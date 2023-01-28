package org.jpalite.dml;

import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;
import org.jpalite.dto.ColumnMetaData;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.jpalite.ReflectionUtils.*;

public class BeanProcessor<T> {

    private final Class<T> clazz;
    private final List<ColumnMetaData> columnMetaData;
    private final List<ColumnMetaData> idColumns;
    private final List<ColumnMetaData> dataColumns;

    public BeanProcessor(Class<T> clazz) throws SQLException {
        this.clazz = clazz;
        validateEntity(clazz);
        List<Field> beanFields = getBeanFields(clazz);
        List<PropertyDescriptor> propertyDescriptors = getPropertyDescriptorsForFields(clazz);
        this.columnMetaData = new ArrayList<>(beanFields.size());

        for (int i = 0; i < beanFields.size(); i++) {
            ColumnMetaData column = new ColumnMetaData();
            column.setColumnIndex(i + 1);

            Field field = beanFields.get(i);
            String columnName = field.getAnnotation(Column.class).name();
            column.setColumnName(columnName);
            String fieldName = field.getName();
            column.setFieldName(fieldName);
            column.setId(field.isAnnotationPresent(Id.class));

            PropertyDescriptor propertyDescriptor = getPropertyDescriptorForField(propertyDescriptors, field);
            if (propertyDescriptor.getReadMethod() != null) {
                column.setReadMethod(propertyDescriptor.getReadMethod());
            } else {
                throw new SQLException(String.format("No suitable getter method found for field %s of class %s", fieldName, clazz.getSimpleName()));
            }

            columnMetaData.add(column);
        }
        idColumns = columnMetaData.stream().filter(ColumnMetaData::isId).toList();
        dataColumns = columnMetaData.stream().filter(i -> !i.isId()).toList();
    }

    private void validateEntity(Class<T> clazz) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new SQLException(String.format("Entity class %s must be @Table annotated", clazz.getSimpleName()));
        }
    }

    public String generateInsertStatement() {
        return "INSERT INTO " + clazz.getAnnotation(Table.class).name()
                + " ("
                + columnMetaData.stream().map(ColumnMetaData::getColumnName).collect(Collectors.joining(", "))
                + ") VALUES ("
                + String.join(", ", Collections.nCopies(columnMetaData.size(), "?"))
                + ")";
    }

    public void fillInsertParameters(PreparedStatement stmt, ParameterMetaData pmd, Object obj) throws SQLException {
        List<Object> params = new ArrayList<>(columnMetaData.size());
        for (var cmd : columnMetaData) {
            params.add(invokeWrapper(cmd.getReadMethod(), obj));
        }
        DMLUtils.fillParameters(stmt, pmd, params.toArray());
    }

    public String generateUpdateStatement() throws SQLException {
        if (idColumns.isEmpty()) {
            throw new SQLException(String.format("Entity class %s has no @Id annotated fields", clazz.getSimpleName()));
        }
        if (dataColumns.isEmpty()) {
            throw new SQLException(String.format("Entity class %s has only @Id annotated fields", clazz.getSimpleName()));
        }
        return "UPDATE "
                + clazz.getAnnotation(Table.class).name()
                + " SET "
                + dataColumns.stream().map(i -> i.getColumnName() + " = ?").collect(Collectors.joining(", "))
                + " WHERE "
                + idColumns.stream().map(i -> i.getColumnName() + " = ?").collect(Collectors.joining(" AND "));
    }

    public void fillUpdateParameters(PreparedStatement stmt, ParameterMetaData pmd, Object obj) throws SQLException {
        List<Object> params = new ArrayList<>(columnMetaData.size());
        for (var cmd : dataColumns) {
            params.add(invokeWrapper(cmd.getReadMethod(), obj));
        }
        for (var cmd : idColumns) {
            params.add(invokeWrapper(cmd.getReadMethod(), obj));
        }
        DMLUtils.fillParameters(stmt, pmd, params.toArray());
    }

}
