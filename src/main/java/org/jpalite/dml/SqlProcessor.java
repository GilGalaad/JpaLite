package org.jpalite.dml;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SqlProcessor {

    private SqlProcessor() {
    }

    public static void fillParameters(PreparedStatement stmt, Object... params) throws SQLException {
        ParameterMetaData parameterMetaData = stmt.getParameterMetaData();
        int stmtCount = parameterMetaData.getParameterCount();
        int paramsCount = params == null ? 0 : params.length;
        if (stmtCount != paramsCount) {
            throw new SQLException(String.format("Query needs %d parameters but %d were provided", stmtCount, paramsCount));
        }
        if (params == null) {
            return;
        }
        DMLUtils.fillParameters(stmt, parameterMetaData, params);
    }

}
