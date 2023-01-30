package org.jpalite;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.jpalite.annotation.Column;
import org.jpalite.annotation.Id;
import org.jpalite.annotation.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

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
        log.info("Fetching null value as primitive array");
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
        log.info("Fetching null value as primitive in bean");
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
        Assertions.assertEquals("ResultSet has 2 columns but class TestBeanWrongColumnNumber has 1 fields", ex.getMessage());
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
        Assertions.assertEquals("No suitable accessor methods found for field col2 of class TestBeanNoSetter", ex.getMessage());
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

    @DisplayName("Fetching too many rows")
    @Test
    void testFetchTooManyRows() throws SQLException {
        log.info("Fetching too many rows");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT, col2 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setString(1, "test_value1");
            stmt.setString(2, "test_value2");
            stmt.executeUpdate();
            stmt.setString(1, "test_value3");
            stmt.setString(2, "test_value4");
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, TestBean.class, "SELECT * FROM test_table"));
        Assertions.assertEquals("Query returned more than 1 row", ex.getMessage());
    }

    @DisplayName("Inserting null bean")
    @Test
    void testInsertNullBean() throws SQLException {
        log.info("Inserting null bean");
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.insert(conn, null));
        Assertions.assertEquals("Bean object is null", ex.getMessage());
    }

    @DisplayName("Inserting null bean list")
    @Test
    void testInsertNullBeanList() throws SQLException {
        log.info("Inserting null bean list");
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.batchInsert(conn, null));
        Assertions.assertEquals("Bean list is empty", ex.getMessage());
    }

    @DisplayName("Inserting bean list with nulls")
    @Test
    void testInsertBeanListWithNulls() throws SQLException {
        log.info("Inserting bean list with nulls");
        List<TestBean> beans = Arrays.asList(new TestBean(), null);
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.batchInsert(conn, beans));
        Assertions.assertEquals("Bean list contains null objects", ex.getMessage());
    }

    @DisplayName("Inserting bean list with different types")
    @Test
    void testInsertBeanListWithDifferentTypes() throws SQLException {
        log.info("Inserting bean list with different types");
        List<Object> beans = Arrays.asList(new TestBean(), new TestBeanPrimitives());
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.batchInsert(conn, beans));
        Assertions.assertEquals("Bean list must contain objects of the same type", ex.getMessage());
    }

    @DisplayName("Updating null bean")
    @Test
    void testUpdateNullBean() throws SQLException {
        log.info("Updating null bean");
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.update(conn, null));
        Assertions.assertEquals("Bean object is null", ex.getMessage());
    }

    @DisplayName("Executing raw sql with wrong parameter number")
    @Test
    void testExecuteWrongParamsNumber() throws SQLException {
        log.info("Executing raw sql with wrong parameter number");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 INTEGER, col2 TEXT)");
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.execute(conn, "UPDATE test_table SET col2 = ? WHERE col1 = ?", "test_param"));
        Assertions.assertEquals("Query needs 2 parameters but 1 were provided", ex.getMessage());
    }

    @DisplayName("Inserting bean with no @Table annotation")
    @Test
    void testInsertBeanNoTableAnnotation() throws SQLException {
        log.info("Inserting bean with no @Table annotation");
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.insert(conn, new TestBeanNoTableAnnotation()));
        Assertions.assertEquals("Bean class TestBeanNoTableAnnotation must be @Table annotated", ex.getMessage());
    }

    @DisplayName("Updating bean with no @Id annotation")
    @Test
    void testUpdateBeanNoIdAnnotation() throws SQLException {
        log.info("Updating bean with no @Id annotation");
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.update(conn, new TestBeanNoIdAnnotation()));
        Assertions.assertEquals("Bean class TestBeanNoIdAnnotation has no @Id annotated fields", ex.getMessage());
    }

    @DisplayName("Updating bean with all @Id annotations")
    @Test
    void testUpdateBeanAllIdAnnotations() throws SQLException {
        log.info("Updating bean with all @Id annotations");
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.update(conn, new TestBeanAllIdAnnotation()));
        Assertions.assertEquals("Bean class TestBeanAllIdAnnotation has only @Id annotated fields", ex.getMessage());
    }

    @DisplayName("Inserting bean with no @Column annotation")
    @Test
    void testInsertBeanNoColumnAnnotation() throws SQLException {
        log.info("Inserting bean with no @Table annotation");
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.insert(conn, new TestBeanNoColumnAnnotation()));
        Assertions.assertEquals("Bean class TestBeanNoColumnAnnotation has no @Column annotated fields", ex.getMessage());
    }

    @DisplayName("Fetching bean with wrong column name")
    @Test
    void testFetchTestBeanWrongColumnName() throws SQLException {
        log.info("Fetching bean with wrong column name");
        execute("CREATE TABLE IF NOT EXISTS test_table (col1 TEXT, col2 TEXT)");
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_table VALUES (?,?)")) {
            stmt.setString(1, "test_value1");
            stmt.setString(2, "test_value2");
            stmt.executeUpdate();
        }
        conn.commit();
        Exception ex = Assertions.assertThrows(SQLException.class, () -> em.getSingleResult(conn, TestBeanWrongColumnName.class, "SELECT * FROM test_table LIMIT 1"));
        Assertions.assertEquals("No suitable field found in class TestBeanWrongColumnName to map column col2", ex.getMessage());
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table(name = "test_table")
    public static class TestBean {

        @Id
        @Column(name = "col1")
        private String col1;
        @Column(name = "col2")
        private String col2;

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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBeanNoTableAnnotation {

        @Column(name = "col1")
        private String col1;
        @Column(name = "col2")
        private String col2;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table(name = "test_table")
    public static class TestBeanNoIdAnnotation {

        @Column(name = "col1")
        private String col1;
        @Column(name = "col2")
        private String col2;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table(name = "test_table")
    public static class TestBeanAllIdAnnotation {

        @Id
        @Column(name = "col1")
        private String col1;
        @Id
        @Column(name = "col2")
        private String col2;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table(name = "test_table")
    public static class TestBeanNoColumnAnnotation {

        private String col1;
        private String col2;

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBeanWrongColumnName {

        @Column(name = "col1")
        private String col1;
        @Column(name = "col3")
        private String col3;

    }

}
