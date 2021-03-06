package org.jpalite.dto;

import lombok.Data;
import org.jpalite.column.ColumnProcessor;

import java.lang.reflect.Method;

@Data
public class ColumnMetaData {

    private int columnIndex;
    private String columnName;
    private String fieldName;
    private boolean isPrimitive;
    private boolean isId;
    private ColumnProcessor<?> columnProcessor;
    private Method readMethod;
    private Method writeMethod;

}
