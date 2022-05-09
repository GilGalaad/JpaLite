package org.jpalite.dml;

import lombok.extern.log4j.Log4j2;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Log4j2
public class SqlProcessor {

    private SqlProcessor() {
    }

    public static void fillParameters(PreparedStatement stmt, Object... params) throws SQLException {
        ParameterMetaData pmd = stmt.getParameterMetaData();
        int stmtCount = pmd.getParameterCount();
        int paramsCount = params == null ? 0 : params.length;
        if (stmtCount != paramsCount) {
            throw new SQLException(String.format("Query needs %d parameters but %d were provided", stmtCount, paramsCount));
        }
        if (params == null) {
            return;
        }
        DMLUtils.fillParameters(stmt, pmd, params);
    }

}
