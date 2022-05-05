package org.jpalite;

import lombok.extern.log4j.Log4j2;
import org.jpalite.model.MyTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@Log4j2
public class TestSuite {

    private static Connection conn;
    private static final EntityManager em = new EntityManager();

    @BeforeAll
    static void init() throws SQLException {
        log.info("Connecting to database");
        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", "localhost", 5432, "postgres");
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", "postgres");
        conn = DriverManager.getConnection(jdbcUrl, jdbcProps);
        conn.setAutoCommit(false);
        dropSchema();
        createSchema();
    }

    static void createSchema() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS public.my_table ("
                         + "my_key BIGINT PRIMARY KEY,"
                         + "string_col TEXT,"
                         + "int_col INTEGER,"
                         + "timestamp_col TIMESTAMP(3)"
                         + ")");
        }
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO public.my_table VALUES (?,?,?,?)")) {
            stmt.setLong(1, 1L);
            stmt.setString(2, "value1");
            stmt.setInt(3, 10);
            stmt.setTimestamp(4, new Timestamp(new java.util.Date().getTime()));
            stmt.addBatch();
            stmt.setLong(1, 2L);
            stmt.setString(2, "value2");
            stmt.setNull(3, Types.INTEGER);
            stmt.setNull(4, Types.TIMESTAMP);
            stmt.addBatch();
            stmt.executeBatch();
        }
        conn.commit();
    }

    static void dropSchema() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS public.my_table");
        }
        conn.commit();
    }

    @AfterAll
    static void cleanup() throws SQLException {
        if (conn != null) {
            conn.setAutoCommit(true);
            dropSchema();
            log.info("Closing database connection");
            conn.close();
        }
    }

    @DisplayName("Fetching list of rows as bean")
    @Test
    void testGetResultListAsBean() throws SQLException {
        List<MyTable> rs = em.getResultList(conn, MyTable.class, "SELECT * FROM my_table ORDER BY 1 LIMIT 5");
        rs.forEach(log::info);
    }

    @DisplayName("Fetching list of rows as object array")
    @Test
    void testGetResultListAsArray() throws SQLException {
        List<Object[]> rs = em.getResultList(conn, Object[].class, "SELECT * FROM my_table ORDER BY 1 LIMIT 5");
        rs.forEach(i -> log.info(Arrays.asList(i)));
    }

    @DisplayName("Fetching single result as bean")
    @Test
    void testSingleResult() throws SQLException {
        MyTable rs = em.getSingleResult(conn, MyTable.class, "SELECT * FROM my_table ORDER BY 1 LIMIT 1");
        log.info(rs);
    }

    @DisplayName("Usage of parameters")
    @Test
    void testGetResultListWithParams() throws SQLException {
        List<MyTable> rs = em.getResultList(conn, MyTable.class, "SELECT * FROM my_table WHERE string_col = ? ORDER BY 1 LIMIT 5", "value2");
        rs.forEach(log::info);
    }

    @DisplayName("Missing parameters")
    @Test
    void testErrorGetResultListWithParams() {
        Exception ex = assertThrows(SQLException.class, () -> {
            List<MyTable> rs = em.getResultList(conn, MyTable.class, "SELECT * FROM my_table WHERE string_col = ? ORDER BY 1 LIMIT 5");
        });
        assertTrue(ex.getMessage().startsWith("Query needs") && ex.getMessage().endsWith("were provided"));
    }

    @DisplayName("Fetching too many rows")
    @Test
    void testTooManyRows() {
        Exception ex = assertThrows(SQLException.class, () -> {
            MyTable rs = em.getSingleResult(conn, MyTable.class, "SELECT * FROM my_table ORDER BY 1 LIMIT 2");
        });
        assertEquals("Query returned more than 1 row", ex.getMessage());
    }

    @DisplayName("Fetching single scalar value")
    @Test
    void testSingleResultScalar() throws SQLException {
        Long count = em.getSingleResult(conn, Long.class, "SELECT COUNT(*) FROM my_table");
        log.info("Count: {}", count);
        List<String> distinct = em.getResultList(conn, String.class, "SELECT DISTINCT string_col FROM my_table ORDER BY 1 LIMIT 5");
        log.info("Distinct values: {}", distinct);
        String value = em.getSingleResult(conn, String.class, "SELECT string_col FROM my_table WHERE 1=2");
        log.info("Null result: {}", value);
    }

    @DisplayName("Fetching all kind of scalar values")
    @Test
    void testAllScalars() throws SQLException {
        String stringVal = em.getSingleResult(conn, String.class, "SELECT string_col FROM my_table ORDER BY 1 LIMIT 1");
        Short shortVal = em.getSingleResult(conn, Short.class, "SELECT int_col FROM my_table ORDER BY 1 LIMIT 1");
        Integer integerVal = em.getSingleResult(conn, Integer.class, "SELECT int_col FROM my_table ORDER BY 1 LIMIT 1");
        Long longVal = em.getSingleResult(conn, Long.class, "SELECT int_col FROM my_table ORDER BY 1 LIMIT 1");
        Date dateVal = em.getSingleResult(conn, Date.class, "SELECT timestamp_col FROM my_table ORDER BY 1 LIMIT 1");
        Object objectVal = em.getSingleResult(conn, Object.class, "SELECT string_col FROM my_table ORDER BY 1 LIMIT 1");
    }

    @DisplayName("Fetching unsupported scalar value")
    @Test
    void testUnsupportedScalar() {
        Exception ex = assertThrows(SQLException.class, () -> {
            EntityManager objectVal = em.getSingleResult(conn, EntityManager.class, "SELECT string_col FROM my_table ORDER BY 1 LIMIT 1");
        });
        assertTrue(ex.getMessage().startsWith("Unsupported column processor for class"));
    }

    @DisplayName("Column number mismatch")
    @Test
    void testColumnNumberMismatch() {
        Exception ex = assertThrows(SQLException.class, () -> {
            MyTable rs = em.getSingleResult(conn, MyTable.class, "SELECT my_key, string_col FROM my_table ORDER BY 1 LIMIT 1");
        });
        assertTrue(ex.getMessage().startsWith("ResultSet has"));
    }

}
