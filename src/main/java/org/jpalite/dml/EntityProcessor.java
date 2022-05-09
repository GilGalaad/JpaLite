package org.jpalite.dml;

import lombok.extern.log4j.Log4j2;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Table;
import org.jpalite.dto.ColumnMetaData;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.jpalite.ReflectionUtils.*;

@Log4j2
public class EntityProcessor<T> extends AbstractDMLProcessor {

    private final Class<T> clazz;
    private final List<ColumnMetaData> cmds;

    public EntityProcessor(Class<T> clazz) throws SQLException {
        this.clazz = clazz;
        validateEntity(clazz);
        List<Field> beanFields = getBeanFields(clazz);
        List<PropertyDescriptor> propertyDescriptors = getPropertyDescriptorsForFields(clazz);
        this.cmds = new ArrayList<>(beanFields.size());

        for (int i = 0; i < beanFields.size(); i++) {
            ColumnMetaData column = new ColumnMetaData();
            column.setColumnIndex(i + 1);

            Field field = beanFields.get(i);
            String columnName = field.getAnnotation(Column.class).name();
            column.setColumnName(columnName);
            String fieldName = field.getName();
            column.setFieldName(fieldName);

            PropertyDescriptor propertyDescriptor = getPropertyDescriptorForField(propertyDescriptors, field);
            if (propertyDescriptor.getReadMethod() != null) {
                column.setReadMethod(propertyDescriptor.getReadMethod());
            } else {
                throw new SQLException(String.format("No suitable getter method found for field %s of class %s", fieldName, clazz.getSimpleName()));
            }

            cmds.add(column);
        }
    }

    public String generateInsertStatement() {
        return "INSERT INTO " + clazz.getAnnotation(Table.class).name()
               + " ("
               + cmds.stream().map(ColumnMetaData::getColumnName).collect(Collectors.joining(", "))
               + ") VALUES ("
               + String.join(", ", Collections.nCopies(cmds.size(), "?"))
               + ")";
    }

    private void validateEntity(Class<T> clazz) throws SQLException {
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new SQLException(String.format("Entity class %s must be @Table annotated", clazz.getSimpleName()));
        }
    }

    public void fillParameters(PreparedStatement stmt, Object obj) throws SQLException {
        Object[] params = new Object[cmds.size()];
        for (var cmd : cmds) {
            try {
                params[cmd.getColumnIndex() - 1] = cmd.getReadMethod().invoke(obj);
            } catch (IllegalAccessException | InvocationTargetException ex) {
                throw new SQLException(String.format("Error invoking setter method %s", cmd.getReadMethod()));
            }
        }
        _fillParameters(stmt, stmt.getParameterMetaData(), params);
    }
}
