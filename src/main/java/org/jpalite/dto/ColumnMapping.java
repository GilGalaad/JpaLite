package org.jpalite.dto;

import lombok.Data;
import org.jpalite.processor.column.ColumnProcessor;

import java.lang.reflect.Method;

@Data
public class ColumnMapping {

    private int columnIndex;
    private String columnLabel;
    private String fieldName;
    private boolean isPrimitive;
    private boolean isId;
    private ColumnProcessor<?> columnProcessor;
    private Method readMethod;
    private Method writeMethod;

}
