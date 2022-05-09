package org.jpalite;

import lombok.extern.log4j.Log4j2;
import org.jpalite.annotation.Column;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Log4j2
public class ReflectionUtils {

    private ReflectionUtils() {
    }

    public static <T> List<Field> getBeanFields(Class<T> clazz) throws SQLException {
        List<Field> ret = Arrays.stream(clazz.getDeclaredFields()).filter(f -> f.isAnnotationPresent(Column.class)).collect(Collectors.toList());
        if (ret.isEmpty()) {
            throw new SQLException(String.format("Entity class %s has no @Column annotated fields", clazz.getSimpleName()));
        }
        return ret;
    }

    public static <T> List<PropertyDescriptor> getPropertyDescriptorsForFields(Class<T> clazz) throws SQLException {
        BeanInfo beanInfo = getBeanInfo(clazz);
        return Arrays.asList(beanInfo.getPropertyDescriptors());
    }

    public static Field getFieldForColumn(List<Field> beanFields, String columnName) throws SQLException {
        Field ret = beanFields.stream()
                .filter(f -> f.getAnnotation(Column.class).name().equals(columnName))
                .findFirst().orElse(null);
        if (ret == null) {
            throw new SQLException(String.format("No suitable field found in class %s to map column %s", beanFields.get(0).getDeclaringClass().getSimpleName(), columnName));
        }
        return ret;
    }

    public static PropertyDescriptor getPropertyDescriptorForField(List<PropertyDescriptor> propertyDescriptors, Field field) throws SQLException {
        PropertyDescriptor ret = propertyDescriptors.stream().filter(pr -> pr.getName().equals(field.getName())).findFirst().orElse(null);
        if (ret == null) {
            throw new SQLException(String.format("Field %s of entity class %s is not a valid property", field.getName(), field.getDeclaringClass().getSimpleName()));
        }
        return ret;
    }

    private static <T> BeanInfo getBeanInfo(Class<T> clazz) throws SQLException {
        try {
            return Introspector.getBeanInfo(clazz);
        } catch (IntrospectionException ex) {
            throw new SQLException("Introspection of " + clazz.getSimpleName() + " class failed", ex);
        }
    }

}
