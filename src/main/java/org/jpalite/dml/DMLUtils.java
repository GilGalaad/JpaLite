package org.jpalite.dml;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DMLUtils {

    private DMLUtils() {
    }

    protected static void fillParameters(PreparedStatement stmt, ParameterMetaData pmd, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            if (params[i] != null) {
                stmt.setObject(i + 1, params[i], pmd.getParameterType(i + 1));
            } else {
                stmt.setNull(i + 1, pmd.getParameterType(i + 1));
            }
        }
    }

}
