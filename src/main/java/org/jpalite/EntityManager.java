package org.jpalite;

import org.jpalite.common.StatementUtils;
import org.jpalite.processor.row.BeanProcessor;
import org.jpalite.processor.row.RowProcessor;
import org.jpalite.processor.row.RowProcessorFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EntityManager {

    private static final int DEFAULT_FETCH_SIZE = 1_000;

    public <T> List<T> getResultList(Connection conn, Class<T> clazz, String sql, Object... params) throws SQLException {
        return getResultList(conn, DEFAULT_FETCH_SIZE, clazz, sql, params);
    }

    public <T> List<T> getResultList(Connection conn, int fetchSize, Class<T> clazz, String sql, Object... params) throws SQLException {
        List<T> ret = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(fetchSize);
            ParameterMetaData parameterMetaData = stmt.getParameterMetaData();
            StatementUtils.checkStatementParameters(parameterMetaData, params);
            StatementUtils.setStatementParameters(stmt, parameterMetaData, params);
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
            ParameterMetaData parameterMetaData = stmt.getParameterMetaData();
            StatementUtils.checkStatementParameters(parameterMetaData, params);
            StatementUtils.setStatementParameters(stmt, parameterMetaData, params);
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
            ParameterMetaData parameterMetaData = stmt.getParameterMetaData();
            StatementUtils.checkStatementParameters(parameterMetaData, params);
            StatementUtils.setStatementParameters(stmt, parameterMetaData, params);
            return stmt.executeLargeUpdate();
        }
    }

    public void insert(Connection conn, Object object) throws SQLException {
        if (object == null) {
            throw new SQLException("Bean object is null");
        }
        BeanProcessor<?> bp = new BeanProcessor<>(object.getClass(), null);
        String sql = bp.generateInsertStatement();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ParameterMetaData parameterMetaData = stmt.getParameterMetaData();
            bp.setInsertParameters(stmt, parameterMetaData, object);
            stmt.executeUpdate();
        }
    }

    public void batchInsert(Connection conn, List<?> objects) throws SQLException {
        if (objects == null || objects.isEmpty()) {
            throw new SQLException("Bean list is empty");
        }
        for (var obj : objects) {
            if (obj == null) {
                throw new SQLException("Bean list contains null objects");
            }
            if (!obj.getClass().equals(objects.getFirst().getClass())) {
                throw new SQLException("Bean list must contain objects of the same type");
            }
        }
        BeanProcessor<?> bp = new BeanProcessor<>(objects.getFirst().getClass(), null);
        String sql = bp.generateInsertStatement();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ParameterMetaData pmd = stmt.getParameterMetaData();
            for (var obj : objects) {
                bp.setInsertParameters(stmt, pmd, obj);
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    public void update(Connection conn, Object object) throws SQLException {
        if (object == null) {
            throw new SQLException("Bean object is null");
        }
        BeanProcessor<?> bp = new BeanProcessor<>(object.getClass(), null);
        String sql = bp.generateUpdateStatement();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ParameterMetaData pmd = stmt.getParameterMetaData();
            bp.setUpdateParameters(stmt, pmd, object);
            stmt.executeUpdate();
        }
    }

}
