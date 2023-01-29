package org.jpalite;

import lombok.*;
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
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setString(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, Exception.class, "SELECT col1 FROM test_table LIMIT 1"));
        Assertions.assertEquals("Unsupported column processor for class Exception", ex.getMessage());
    }

    @DisplayName("Fetching null value as primitive scalar")
    @Test
    void testFetchNullValueAsPrimitiveScalar() throws SQLException {
        log.info("Fetching null value as primitive scalar");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setObject(1, null);
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, int.class, "SELECT col1 FROM test_table LIMIT 1"));
        Assertions.assertEquals("Cannot assign null value to a primitive type for column col1", ex.getMessage());
    }

    @DisplayName("Fetching null value as primitive array")
    @Test
    void testFetchNullValueAsPrimitiveArray() throws SQLException {
        log.info("Fetching null value as primitive scalar");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER, col2 INTEGER)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setObject(1, 1);
            stmt.setObject(2, null);
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, int[].class, "SELECT * FROM test_table LIMIT 1"));
        Assertions.assertEquals("Cannot assign null value to a primitive type for column col2", ex.getMessage());
    }

    @DisplayName("Fetching null value as primitive in bean")
    @Test
    void testFetchNullValueAsPrimitiveInBean() throws SQLException {
        log.info("Fetching null value as primitive scalar");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER, col2 INTEGER)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setObject(1, 1);
            stmt.setObject(2, null);
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, TestBeanPrimitives.class, "SELECT * FROM test_table LIMIT 1"));
        Assertions.assertEquals("Cannot assign null value to a primitive type for column col2", ex.getMessage());
    }

    @DisplayName("Fetching bean with wrong column number")
    @Test
    void testFetchTestBeanWrongColumnNumber() throws SQLException {
        log.info("Fetching bean with wrong column number");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT, col2 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setString(1, "test_value1");
            stmt.setString(2, "test_value2");
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, TestBeanWrongColumnNumber.class, "SELECT * FROM test_table LIMIT 1"));
        Assertions.assertEquals("ResultSet has 2 columns but TestBeanWrongColumnNumber has 1 fields", ex.getMessage());
    }

    @DisplayName("Fetching bean with no setter")
    @Test
    void testFetchTestBeanNoSetter() throws SQLException {
        log.info("Fetching bean with no setter");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT, col2 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setString(1, "test_value1");
            stmt.setString(2, "test_value2");
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, TestBeanNoSetter.class, "SELECT * FROM test_table LIMIT 1"));
        Assertions.assertEquals("No suitable setter method found for field col2 of class TestBeanNoSetter", ex.getMessage());
    }

    @DisplayName("Fetching bean with no default constructor")
    @Test
    void testFetchTestBeanNoDefaultConstructor() throws SQLException {
        log.info("Fetching bean with no default constructor");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT, col2 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setString(1, "test_value1");
            stmt.setString(2, "test_value2");
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, TestBeanNoDefaultConstructor.class, "SELECT * FROM test_table LIMIT 1"));
        Assertions.assertEquals("No default constructor found in class TestBeanNoDefaultConstructor", ex.getMessage());
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBeanPrimitives {

        @Column(name = "col1")
        private int col1;
        @Column(name = "col2")
        private int col2;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBeanWrongColumnNumber {

        @Column(name = "col1")
        private String col1;

    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBeanNoSetter {

        @Getter
        @Setter
        @Column(name = "col1")
        private String col1;
        @Getter
        @Column(name = "col2")
        private Integer col2;

    }

    @Data
    @AllArgsConstructor
    public static class TestBeanNoDefaultConstructor {

        @Column(name = "col1")
        private String col1;
        @Column(name = "col2")
        private Integer col2;

    }

}
