package org.jpalite;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TestEntityManager extends TestSession {

    @DisplayName("Fetching no row")
    @Test
    void testFetchNoRow() throws SQLException {
        log.info("Fetching no row");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT)");
        String actual = em.getSingleResult(conn, String.class, "SELECT col1 FROM test_table LIMIT 1");
        Assertions.assertNull(actual);
    }

    @DisplayName("Executing raw sql")
    @Test
    void testExecute() throws SQLException {
        log.info("Executing raw sql");
        long affected = em.execute(conn, "CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER, col2 TEXT)");
        Assertions.assertEquals(0L, affected);
        conn.commit();
        long actual = em.getSingleResult(conn, long.class, "SELECT COUNT(*) FROM test_table");
        Assertions.assertEquals(0L, actual);
        em.execute(conn, "INSERT INTO test_table VALUES (?,?)", 1, "test_value2");
        conn.commit();
        actual = em.getSingleResult(conn, long.class, "SELECT COUNT(*) FROM test_table");
        Assertions.assertEquals(1L, actual);
    }

    @DisplayName("Inserting bean")
    @Test
    void testInsertBean() throws SQLException {
        log.info("Inserting bean");
        TestBean expected = new TestBean(1, "test_value");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER, col2 TEXT)");
        em.insert(conn, expected);
        conn.commit();
        TestBean actual = em.getSingleResult(conn, TestBean.class, "SELECT * FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
    }

    @DisplayName("Updating bean")
    @Test
    void testUpdateBean() throws SQLException {
        log.info("Updating bean");
        TestBean expected = new TestBean(1, "test_value");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER, col2 TEXT)");
        em.insert(conn, expected);
        conn.commit();
        expected.setCol2(null);
        em.update(conn, expected);
        TestBean actual = em.getSingleResult(conn, TestBean.class, "SELECT * FROM test_table LIMIT 1");
        Assertions.assertEquals(expected, actual);
    }

    @DisplayName("Batch inserting")
    @Test
    void testBatchInsert() throws SQLException {
        log.info("Batch inserting");
        long startTime, endTime;
        int batchSize = 100_000;
        int count;

        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER, col2 TEXT)");
        startTime = System.nanoTime();
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            for (int i = 0; i < batchSize; i++) {
                stmt.setInt(1, i + 1);
                stmt.setString(2, "test");
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
        conn.commit();
        endTime = System.nanoTime();
        count = em.getSingleResult(conn, int.class, "SELECT COUNT(*) FROM test_table");
        Assertions.assertEquals(batchSize, count);
        log.info("Inserted {} rows with JDBC: {} ms", count, (endTime - startTime) / 1_000_000);

        execute("DROP TABLE IF EXISTS test_table");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER, col2 TEXT)");
        List<TestBean> beans = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            TestBean bean = new TestBean(i + 1, "test");
            beans.add(bean);
        }
        startTime = System.nanoTime();
        em.batchInsert(conn, beans);
        conn.commit();
        endTime = System.nanoTime();
        count = em.getSingleResult(conn, Integer.class, "SELECT COUNT(*) FROM test_table");
        Assertions.assertEquals(batchSize, count);
        log.info("Inserted {} rows with JpaLite: {} ms", count, (endTime - startTime) / 1_000_000);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table(name = "test_table")
    public static class TestBean {

        @Id
        @Column(name = "col1")
        private Integer col1;
        @Column(name = "col2")
        private String col2;

    }

}
