package org.jpalite;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpalite.annotation.Column;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
public class TestExceptions extends TestSession {

    @DisplayName("Fetching unsupported type as scalar")
    @Test
    void testFetchUnsupportedTypeAsScalar() throws SQLException {
        log.info("Fetching unsupported type as scalar");
        String expected = "test_value";
        execute("CREATE TABLE IF NOT EXISTS test_table (string_col TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setString(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, Exception.class, "SELECT string_col FROM test_table LIMIT 1"));
        Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().startsWith("Unsupported column processor for class"));
    }

    @DisplayName("Fetching null value as primitive scalar")
    @Test
    void testFetchNullValueAsPrimitiveScalar() throws SQLException {
        log.info("Fetching null value as primitive scalar");
        execute("CREATE TABLE IF NOT EXISTS test_table (integer_col INTEGER)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setObject(1, null);
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, int.class, "SELECT integer_col FROM test_table LIMIT 1"));
        Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().startsWith("Cannot assign null value to a primitive type"));
    }

    @DisplayName("Fetching null value as primitive array")
    @Test
    void testFetchNullValueAsPrimitiveArray() throws SQLException {
        log.info("Fetching null value as primitive scalar");
        execute("CREATE TABLE IF NOT EXISTS test_table (integer_col1 INTEGER, integer_col2 INTEGER)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setObject(1, 1);
            stmt.setObject(2, null);
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, int[].class, "SELECT * FROM test_table LIMIT 1"));
        Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().startsWith("Cannot assign null value to a primitive type"));
    }

    @DisplayName("Fetching null value as primitive in bean")
    @Test
    void testFetchNullValueAsPrimitiveInBean() throws SQLException {
        log.info("Fetching null value as primitive scalar");
        execute("CREATE TABLE IF NOT EXISTS test_table (integer_col1 INTEGER, integer_col2 INTEGER)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setObject(1, 1);
            stmt.setObject(2, null);
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, TestBeanPrimitives.class, "SELECT * FROM test_table LIMIT 1"));
        Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().startsWith("Cannot assign null value to a primitive type"));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBeanPrimitives {

        @Column(name = "integer_col1")
        private int integerCol1;
        @Column(name = "integer_col2")
        private int integerCol2;

    }

}
