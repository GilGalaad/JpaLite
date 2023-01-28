package org.jpalite.row;

import org.jpalite.column.ColumnProcessorFactory;
import org.jpalite.dto.ColumnMetaData;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ArrayProcessor<T> implements RowProcessor<T> {

    private final Class<?> componentClass;
    private final List<ColumnMetaData> cmds;

    public ArrayProcessor(Class<T> clazz, ResultSetMetaData rsmd) throws SQLException {
        this.componentClass = clazz.getComponentType();
        this.cmds = new ArrayList<>(rsmd.getColumnCount());

        for (int i = 0; i < rsmd.getColumnCount(); i++) {
            ColumnMetaData column = new ColumnMetaData();
            column.setColumnIndex(i + 1);

            String columnName = rsmd.getColumnLabel(i + 1);
            column.setColumnName(columnName);

            column.setPrimitive(componentClass.isPrimitive());
            column.setColumnProcessor(ColumnProcessorFactory.create(componentClass));

            cmds.add(column);
        }

    }

    @Override
    public T process(ResultSet rs) throws SQLException {
        @SuppressWarnings("unchecked")
        T ret = (T) Array.newInstance(componentClass, cmds.size());
        for (var cmd : cmds) {
            Object value = cmd.getColumnProcessor().process(rs, cmd.getColumnIndex());
            if (cmd.isPrimitive() && value == null) {
                throw new SQLException(String.format("Cannot assign null value to a primitive type for column %s", cmd.getColumnName()));
            }
            Array.set(ret, cmd.getColumnIndex() - 1, value);
        }
        return ret;
    }

}
