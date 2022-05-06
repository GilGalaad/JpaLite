package org.jpalite;

import lombok.extern.log4j.Log4j2;
import org.jpalite.row.RowProcessor;
import org.jpalite.row.RowProcessorFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Log4j2
public class EntityManager {

    private static final int DEFAULT_FETCH_SIZE = 10_000;

    public <T> List<T> getResultList(Connection conn, Class<T> clazz, String sql, Object... params) throws SQLException {
        return getResultList(conn, DEFAULT_FETCH_SIZE, clazz, sql, params);
    }

    public <T> List<T> getResultList(Connection conn, int fetchSize, Class<T> clazz, String sql, Object... params) throws SQLException {
        List<T> ret = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(fetchSize);
            fillParameters(stmt, params);
            try (ResultSet rs = stmt.executeQuery()) {
                RowProcessor<T> rowProcessor = RowProcessorFactory.create(clazz, rs.getMetaData());
                while (rs.next()) {
                    ret.add(rowProcessor.process(rs));
                }
            }
        }
        return ret.isEmpty() ? Collections.emptyList() : ret;
    }

    public <T> T getSingleResult(Connection conn, Class<T> clazz, String sql, Object... params) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(2);
            fillParameters(stmt, params);
            try (ResultSet rs = stmt.executeQuery()) {
                RowProcessor<T> rowProcessor = RowProcessorFactory.create(clazz, rs.getMetaData());
                if (rs.next()) {
                    T ret = rowProcessor.process(rs);
                    if (rs.next()) {
                        throw new SQLException("Query returned more than 1 row");
                    }
                    return ret;
                } else {
                    return null;
                }
            }
        }
    }

    public long execute(Connection conn, String sql, Object... params) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            fillParameters(stmt, params);
            return stmt.executeLargeUpdate();
        }
    }

    private void fillParameters(PreparedStatement stmt, Object... params) throws SQLException {
        ParameterMetaData pmd = stmt.getParameterMetaData();
        int stmtCount = pmd.getParameterCount();
        int paramsCount = params == null ? 0 : params.length;
        if (stmtCount != paramsCount) {
            throw new SQLException(String.format("Query needs %d parameters but %d were provided", stmtCount, paramsCount));
        }
        if (params == null) {
            return;
        }
        for (int i = 0; i < params.length; i++) {
            if (params[i] != null) {
                stmt.setObject(i + 1, params[i]);
            } else {
                int sqlType = pmd.getParameterType(i + 1);
                stmt.setNull(i + 1, sqlType);
            }
        }
    }

}
