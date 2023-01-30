package org.jpalite.common;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StatementUtils {

    private StatementUtils() {
    }

    public static void checkStatementParameters(PreparedStatement stmt, ParameterMetaData parameterMetaData, Object... params) throws SQLException {
        int stmtCount = parameterMetaData.getParameterCount();
        int paramsCount = params == null ? 0 : params.length;
        if (stmtCount != paramsCount) {
            throw new SQLException(String.format("Query needs %d parameters but %d were provided", stmtCount, paramsCount));
        }
    }

    public static void setStatementParameters(PreparedStatement stmt, ParameterMetaData parameterMetaData, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            if (params[i] != null) {
                stmt.setObject(i + 1, params[i], parameterMetaData.getParameterType(i + 1));
            } else {
                stmt.setNull(i + 1, parameterMetaData.getParameterType(i + 1));
            }
        }
    }

}
