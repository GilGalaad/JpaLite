package org.jpalite;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

@Slf4j
public class TestColumnProcessor extends TestSession {

    @DisplayName("Fetching string as scalar")
    @Test
    void testFetchStringAsScalar() throws SQLException {
        log.info("Fetching string as scalar");
        String expected = "test_value";
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setString(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        String actual = em.getSingleResult(conn, String.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
        execute("UPDATE test_table SET col1 = NULL");
        actual = em.getSingleResult(conn, String.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

    @DisplayName("Fetching short as scalar")
    @Test
    void testFetchShortAsScalar() throws SQLException {
        log.info("Fetching short as scalar");
        Short expected = 1;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 SMALLINT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setShort(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        Short actual = em.getSingleResult(conn, Short.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
        execute("UPDATE test_table SET col1 = NULL");
        actual = em.getSingleResult(conn, Short.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

    @DisplayName("Fetching short as primitive scalar")
    @Test
    void testFetchShortAsPrimitiveScalar() throws SQLException {
        log.info("Fetching short as primitive scalar");
        short expected = 1;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 SMALLINT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setShort(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        short actual = em.getSingleResult(conn, short.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
    }

    @DisplayName("Fetching integer as scalar")
    @Test
    void testFetchIntegerAsScalar() throws SQLException {
        log.info("Fetching integer as scalar");
        Integer expected = 1;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setInt(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        Integer actual = em.getSingleResult(conn, Integer.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
        execute("UPDATE test_table SET col1 = NULL");
        actual = em.getSingleResult(conn, Integer.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

    @DisplayName("Fetching integer as primitive scalar")
    @Test
    void testFetchIntegerAsPrimitiveScalar() throws SQLException {
        log.info("Fetching integer column as primitive scalar");
        int expected = 1;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setInt(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        int actual = em.getSingleResult(conn, int.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
    }

    @DisplayName("Fetching long as scalar")
    @Test
    void testFetchLongAsScalar() throws SQLException {
        log.info("Fetching long as scalar");
        Long expected = 1L;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 BIGINT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setLong(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        Long actual = em.getSingleResult(conn, Long.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
        execute("UPDATE test_table SET col1 = NULL");
        actual = em.getSingleResult(conn, Long.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

    @DisplayName("Fetching long as primitive scalar")
    @Test
    void testFetchLongAsPrimitiveScalar() throws SQLException {
        log.info("Fetching long as primitive scalar");
        long expected = 1L;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 BIGINT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setLong(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        long actual = em.getSingleResult(conn, long.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
    }

    @DisplayName("Fetching float as scalar")
    @Test
    void testFetchFloatAsScalar() throws SQLException {
        log.info("Fetching float as scalar");
        Float expected = 3.14F;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 DECIMAL)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setFloat(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        Float actual = em.getSingleResult(conn, Float.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
        execute("UPDATE test_table SET col1 = NULL");
        actual = em.getSingleResult(conn, Float.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

    @DisplayName("Fetching float as primitive scalar")
    @Test
    void testFetchFloatAsPrimitiveScalar() throws SQLException {
        log.info("Fetching float as primitive scalar");
        float expected = 3.14F;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 DECIMAL)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setFloat(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        float actual = em.getSingleResult(conn, float.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
    }

    @DisplayName("Fetching double as scalar")
    @Test
    void testFetchDoubleAsScalar() throws SQLException {
        log.info("Fetching double as scalar");
        Double expected = 3.14D;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 DECIMAL)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setDouble(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        Double actual = em.getSingleResult(conn, Double.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
        execute("UPDATE test_table SET col1 = NULL");
        actual = em.getSingleResult(conn, Double.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

    @DisplayName("Fetching double as primitive scalar")
    @Test
    void testFetchDoubleAsPrimitiveScalar() throws SQLException {
        log.info("Fetching double as primitive scalar");
        double expected = 3.14D;
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 DECIMAL)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setDouble(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        double actual = em.getSingleResult(conn, double.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
    }

    @DisplayName("Fetching big decimal as scalar")
    @Test
    void testFetchBigDecimalAsScalar() throws SQLException {
        log.info("Fetching big decimal as scalar");
        BigDecimal expected = BigDecimal.valueOf(3.14D);
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 DECIMAL)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setBigDecimal(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        BigDecimal actual = em.getSingleResult(conn, BigDecimal.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
        execute("UPDATE test_table SET col1 = NULL");
        actual = em.getSingleResult(conn, BigDecimal.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

    @DisplayName("Fetching date as scalar")
    @Test
    void testFetchDateAsScalar() throws SQLException {
        log.info("Fetching date as scalar");
        Date expected = new Date();
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TIMESTAMP(3))");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setTimestamp(1, new java.sql.Timestamp(expected.getTime()));
            stmt.executeUpdate();
        }
        conn.commit();
        Date actual = em.getSingleResult(conn, Date.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
        execute("UPDATE test_table SET col1 = NULL");
        actual = em.getSingleResult(conn, Date.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

    @DisplayName("Fetching generic object as scalar")
    @Test
    void testFetchObjectAsScalar() throws SQLException {
        log.info("Fetching generic object as scalar");
        Object expected = "test_value";
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?)")) {
            stmt.setObject(1, expected);
            stmt.executeUpdate();
        }
        conn.commit();
        Object actual = em.getSingleResult(conn, Object.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertEquals(expected.getClass().getName(), actual.getClass().getName());
        Assertions.assertEquals(expected, actual);
        execute("UPDATE test_table SET col1 = NULL");
        actual = em.getSingleResult(conn, Object.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

}
