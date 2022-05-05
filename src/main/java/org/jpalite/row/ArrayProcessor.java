package org.jpalite.row;

import lombok.extern.log4j.Log4j2;
import org.jpalite.column.ColumnProcessor;
import org.jpalite.column.ColumnProcessorFactory;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Log4j2
public class ArrayProcessor<T> implements RowProcessor<T> {

    private final Class<?> componentClass;
    private final int columnCount;
    private final ColumnProcessor<?> columnProcessor;

    public ArrayProcessor(Class<T> clazz, ResultSetMetaData rsmd) throws SQLException {
        this.componentClass = clazz.getComponentType();
        this.columnCount = rsmd.getColumnCount();
        this.columnProcessor = ColumnProcessorFactory.create(componentClass);
    }

    @Override
    public T process(ResultSet rs) throws SQLException {
        @SuppressWarnings("unchecked")
        T ret = (T) Array.newInstance(componentClass, columnCount);
        for (int i = 0; i < columnCount; i++) {
            Array.set(ret, i, columnProcessor.process(rs, i + 1));
        }
        return ret;
    }

}
