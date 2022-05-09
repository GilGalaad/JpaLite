package org.jpalite;

import lombok.extern.log4j.Log4j2;
import org.jpalite.dml.EntityProcessor;
import org.jpalite.dml.SqlProcessor;
import org.jpalite.row.RowProcessor;
import org.jpalite.row.RowProcessorFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
        EntityProcessor<?> ep = new EntityProcessor<>(obj.getClass());
        String sql = ep.generateInsertStatement();
        log.info(sql);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ep.fillInsertParameters(stmt, obj);
            stmt.executeUpdate();
        }
    }

    public void update(Connection conn, Object obj) throws SQLException {
        if (obj == null) {
            throw new SQLException("Entity object is null");
        }
        EntityProcessor<?> ep = new EntityProcessor<>(obj.getClass());
        String sql = ep.generateUpdateStatement();
        log.info(sql);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            ep.fillUpdateParameters(stmt, obj);
            stmt.executeUpdate();
        }
    }

}
