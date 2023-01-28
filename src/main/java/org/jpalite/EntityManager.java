package org.jpalite;

import org.jpalite.dml.BeanProcessor;
import org.jpalite.dml.SqlProcessor;
import org.jpalite.row.RowProcessor;
import org.jpalite.row.RowProcessorFactory;

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
            SqlProcessor.fillParameters(stmt, params);
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
            SqlProcessor.fillParameters(stmt, params);
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
            SqlProcessor.fillParameters(stmt, params);
            return stmt.executeLargeUpdate();
        }
    }

    public void insert(Connection conn, Object obj) throws SQLException {
        if (obj == null) {
            throw new SQLException("Entity object is null");
        }
        BeanProcessor<?> bp = new BeanProcessor<>(obj.getClass());
        String sql = bp.generateInsertStatement();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ParameterMetaData pmd = stmt.getParameterMetaData();
            bp.fillInsertParameters(stmt, pmd, obj);
            stmt.executeUpdate();
        }
    }

    public void batchInsert(Connection conn, List<?> objs) throws SQLException {
        if (objs == null || objs.isEmpty()) {
            throw new SQLException("Entity list is empty");
        }
        for (var obj : objs) {
            if (obj == null) {
                throw new SQLException("Entity list contains null objects");
            }
            if (!obj.getClass().equals(objs.get(0).getClass())) {
                throw new SQLException("Entity list must contain objects of the same type");
            }
        }
        BeanProcessor<?> bp = new BeanProcessor<>(objs.get(0).getClass());
        String sql = bp.generateInsertStatement();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ParameterMetaData pmd = stmt.getParameterMetaData();
            for (var obj : objs) {
                bp.fillInsertParameters(stmt, pmd, obj);
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    public void update(Connection conn, Object obj) throws SQLException {
        if (obj == null) {
            throw new SQLException("Entity object is null");
        }
        BeanProcessor<?> bp = new BeanProcessor<>(obj.getClass());
        String sql = bp.generateUpdateStatement();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ParameterMetaData pmd = stmt.getParameterMetaData();
            bp.fillUpdateParameters(stmt, pmd, obj);
            stmt.executeUpdate();
        }
    }

}
