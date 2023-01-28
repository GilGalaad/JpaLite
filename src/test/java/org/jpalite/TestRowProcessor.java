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
import java.util.Arrays;
import java.util.List;

@Slf4j
public class TestRowProcessor extends TestSession {

    @DisplayName("Fetching single row as bean")
    @Test
    void testFetchSingleRowAsBean() throws SQLException {
        log.info("Fetching single row as bean");
        TestBean expected = new TestBean("test_value1", "test_value2");
        execute("CREATE TABLE IF NOT EXISTS test_table (string_col1 TEXT, string_col2 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setString(1, expected.stringCol1);
            stmt.setString(2, expected.stringCol2);
            stmt.executeUpdate();
        }
        conn.commit();
        TestBean actual = em.getSingleResult(conn, TestBean.class, "SELECT * FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
    }

    @DisplayName("Fetching single row as array")
    @Test
    void testFetchSingleRowAsArray() throws SQLException {
        log.info("Fetching single row as array");
        String[] expected = new String[]{"test_value1", "test_value2"};
        execute("CREATE TABLE IF NOT EXISTS test_table (string_col1 TEXT, string_col2 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setString(1, expected[0]);
            stmt.setString(2, expected[1]);
            stmt.executeUpdate();
        }
        conn.commit();
        String[] actual = em.getSingleResult(conn, String[].class, "SELECT * FROM test_table LIMIT 1");
        Assertions.assertArrayEquals(expected, actual);
    }

    @DisplayName("Fetching multiple rows as list of beans")
    @Test
    void testFetchMultipleRowsAsListOfBeans() throws SQLException {
        log.info("Fetching multiple rows as list of beans");
        List<TestBean> expected = Arrays.asList(new TestBean("test_value1", "test_value2"), new TestBean("test_value3", "test_value4"));
        execute("CREATE TABLE IF NOT EXISTS test_table (string_col1 TEXT, string_col2 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setString(1, expected.get(0).stringCol1);
            stmt.setString(2, expected.get(0).stringCol2);
            stmt.executeUpdate();
            stmt.setString(1, expected.get(1).stringCol1);
            stmt.setString(2, expected.get(1).stringCol2);
            stmt.executeUpdate();
        }
        conn.commit();
        List<TestBean> actual = em.getResultList(conn, TestBean.class, "SELECT * FROM test_table ORDER BY 1");
        Assertions.assertIterableEquals(expected, actual);
    }

    @DisplayName("Fetching multiple rows as list of arrays")
    @Test
    void testFetchMultipleRowsAsListOfArrays() throws SQLException {
        log.info("Fetching multiple rows as list of arrays");
        List<String[]> expected = Arrays.asList(new String[]{"test_value1", "test_value2"}, new String[]{"test_value3", "test_value4"});
        execute("CREATE TABLE IF NOT EXISTS test_table (string_col1 TEXT, string_col2 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setString(1, expected.get(0)[0]);
            stmt.setString(2, expected.get(0)[1]);
            stmt.executeUpdate();
            stmt.setString(1, expected.get(1)[0]);
            stmt.setString(2, expected.get(1)[1]);
            stmt.executeUpdate();
        }
        conn.commit();
        List<String[]> actual = em.getResultList(conn, String[].class, "SELECT * FROM test_table ORDER BY 1");
        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertArrayEquals(expected.get(i), actual.get(i));
        }
    }

    @DisplayName("Fetching multiple rows as list of scalars")
    @Test
    void testFetchMultipleRowsAsListOfScalars() throws SQLException {
        log.info("Fetching multiple rows as list of scalars");
        List<String> expected = Arrays.asList("test_value1", "test_value2");
        execute("CREATE TABLE IF NOT EXISTS test_table (string_col TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setString(1, expected.get(0));
            stmt.executeUpdate();
            stmt.setString(1, expected.get(1));
            stmt.executeUpdate();
        }
        conn.commit();
        List<String> actual = em.getResultList(conn, String.class, "SELECT * FROM test_table ORDER BY 1");
        Assertions.assertIterableEquals(expected, actual);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBean {

        @Column(name = "string_col1")
        private String stringCol1;
        @Column(name = "string_col2")
        private String stringCol2;

    }

}
