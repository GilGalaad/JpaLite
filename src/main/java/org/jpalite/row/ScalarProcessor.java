package org.jpalite.row;

import lombok.extern.log4j.Log4j2;
import org.jpalite.column.ColumnProcessorFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Log4j2
public class ScalarProcessor<T> implements RowProcessor<T> {

    private final Class<T> clazz;
    private final ColumnMetaData cmd;

    public ScalarProcessor(Class<T> clazz, ResultSetMetaData rsmd) throws SQLException {
        this.clazz = clazz;
        this.cmd = new ColumnMetaData();
        cmd.setColumnIndex(1);
        cmd.setColumnName(rsmd.getColumnLabel(1));
        cmd.setColumnProcessor(ColumnProcessorFactory.create(clazz));
    }

    @Override
    public T process(ResultSet rs) throws SQLException {
        @SuppressWarnings("unchecked")
        T value = (T) cmd.getColumnProcessor().process(rs, 1);
        if (clazz.isPrimitive() && value == null) {
            throw new SQLException(String.format("Cannot assign null value to a primitive type for column %s", cmd.getColumnName()));
        }
        return value;
    }

}
