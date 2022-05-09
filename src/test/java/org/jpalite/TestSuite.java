package org.jpalite;

import lombok.extern.log4j.Log4j2;
import org.jpalite.model.*;
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
    private final EntityManager em = new EntityManager();

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
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS public.my_table ("
                               + "my_key INTEGER PRIMARY KEY,"
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
            stmt.executeUpdate("DROP TABLE IF EXISTS public.my_table");
            stmt.executeUpdate("DROP TABLE IF EXISTS public.my_table2");
        }
        conn.commit();
    }

    @AfterAll
    static void cleanup() throws SQLException {
        if (conn != null) {
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

    @DisplayName("Fetching list of rows as bean with primitive fields")
    @Test
    void testGetResultListAsBeanWithPrimitives() throws SQLException {
        List<MyTableWithPrimitives> rs = em.getResultList(conn, MyTableWithPrimitives.class, "SELECT * FROM my_table WHERE int_col IS NOT NULL ORDER BY 1 LIMIT 5");
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
            EntityManager rs = em.getSingleResult(conn, EntityManager.class, "SELECT string_col FROM my_table ORDER BY 1 LIMIT 1");
        });
        log.error(ex.getMessage());
        assertEquals("Unsupported column processor for class EntityManager", ex.getMessage());
    }

    @DisplayName("Fetching single null value")
    @Test
    void testNullScalar() throws SQLException {
        Object value = em.getSingleResult(conn, Object.class, "SELECT my_key FROM my_table WHERE 1=2");
        assertNull(value);

        MyTable rs = em.getSingleResult(conn, MyTable.class, "SELECT * FROM my_table WHERE 1=2");
        assertNull(rs);
    }

    @DisplayName("Usage of parameters")
    @Test
    void testUsageOfParameters() throws SQLException {
        List<MyTable> rs = em.getResultList(conn, MyTable.class, "SELECT * FROM my_table WHERE string_col = ? ORDER BY 1 LIMIT 5", "value2");
        rs.forEach(log::info);
    }

    @DisplayName("Missing parameters")
    @Test
    void testErrorGetResultListWithParams() {
        Exception ex = assertThrows(SQLException.class, () -> {
            List<MyTable> rs = em.getResultList(conn, MyTable.class, "SELECT * FROM my_table WHERE string_col = ? ORDER BY 1 LIMIT 5");
        });
        log.error(ex.getMessage());
        assertEquals("Query needs 1 parameters but 0 were provided", ex.getMessage());
    }

    @DisplayName("Fetching nulls on primitive types")
    @Test
    void testNullsOnPrimitives() {
        Exception ex = assertThrows(SQLException.class, () -> {
            List<MyTableWithPrimitives> rs = em.getResultList(conn, MyTableWithPrimitives.class, "SELECT * FROM my_table WHERE int_col IS NULL ORDER BY 1 LIMIT 5");
        });
        log.error(ex.getMessage());
        assertEquals("Cannot assign null value to a primitive type for column int_col", ex.getMessage());

        ex = assertThrows(SQLException.class, () -> {
            int value = em.getSingleResult(conn, Integer.TYPE, "SELECT int_col FROM my_table WHERE int_col IS NULL");
        });
        log.error(ex.getMessage());
        assertEquals("Cannot assign null value to a primitive type for column int_col", ex.getMessage());

        ex = assertThrows(SQLException.class, () -> {
            int[] value = em.getSingleResult(conn, int[].class, "SELECT my_key, int_col FROM my_table WHERE int_col IS NULL");
        });
        log.error(ex.getMessage());
        assertEquals("Cannot assign null value to a primitive type for column int_col", ex.getMessage());
    }

    @DisplayName("Wrong entities in select")
    @Test
    void testWrongEntities() {
        Exception ex = assertThrows(SQLException.class, () -> {
            MyTableWithoutConstructor rs = em.getSingleResult(conn, MyTableWithoutConstructor.class, "SELECT * FROM my_table ORDER BY 1 LIMIT 1");
        });
        log.error(ex.getMessage());
        assertEquals("No default constructor found in class MyTableWithoutConstructor", ex.getMessage());

        ex = assertThrows(SQLException.class, () -> {
            MyTableWithUnmappedField rs = em.getSingleResult(conn, MyTableWithUnmappedField.class, "SELECT * FROM my_table ORDER BY 1 LIMIT 1");
        });
        log.error(ex.getMessage());
        assertEquals("ResultSet has 4 columns but MyTableWithUnmappedField has 3 fields", ex.getMessage());

        ex = assertThrows(SQLException.class, () -> {
            MyTableWithWrongMapping rs = em.getSingleResult(conn, MyTableWithWrongMapping.class, "SELECT * FROM my_table ORDER BY 1 LIMIT 1");
        });
        log.error(ex.getMessage());
        assertEquals("No suitable field found in class MyTableWithWrongMapping to map column int_col", ex.getMessage());

        ex = assertThrows(SQLException.class, () -> {
            MyTableWithoutAccessors rs = em.getSingleResult(conn, MyTableWithoutAccessors.class, "SELECT * FROM my_table ORDER BY 1 LIMIT 1");
        });
        log.error(ex.getMessage());
        assertEquals("No suitable setter method found for field myKey of class MyTableWithoutAccessors", ex.getMessage());
    }

    @DisplayName("Executing arbitrary statements")
    @Test
    void testExecute() throws SQLException {
        em.execute(conn, "CREATE TABLE IF NOT EXISTS public.my_table2 (my_key INTEGER)");
        em.execute(conn, "INSERT INTO my_table2 VALUES (?)", 10);
        int count = em.getSingleResult(conn, Integer.class, "SELECT COUNT(*) FROM my_table2");
        assertEquals(1, count);
        int value = em.getSingleResult(conn, Integer.class, "SELECT my_key FROM my_table2 LIMIT 1");
        assertEquals(10, value);
        em.execute(conn, "DROP TABLE public.my_table2");
        conn.rollback();
    }

    @DisplayName("Inserting entity")
    @Test
    void testInsert() throws SQLException {
        MyTable insert = new MyTable();
        insert.setMyKey(10L);
        insert.setStringCol("new value");
        insert.setIntCol(null);
        insert.setTimestampCol(new Date());
        em.insert(conn, insert);
        MyTable rs = em.getSingleResult(conn, MyTable.class, "SELECT * FROM my_table WHERE my_key = ?", insert.getMyKey());
        log.info(rs);
        assertEquals(insert, rs);
        conn.rollback();
    }

    @DisplayName("Wrong entities in DML")
    @Test
    void testWrongEntitiesInDML() throws SQLException {
        Exception ex = assertThrows(SQLException.class, () -> em.insert(conn, null));
        log.error(ex.getMessage());
        assertEquals("Entity object is null", ex.getMessage());

        ex = assertThrows(SQLException.class, () -> em.insert(conn, new MyTableUnannotated()));
        log.error(ex.getMessage());
        assertEquals("Entity class MyTableUnannotated must be @Table annotated", ex.getMessage());

        ex = assertThrows(SQLException.class, () -> em.insert(conn, new MyTableWithoutAccessors()));
        log.error(ex.getMessage());
        assertEquals("No suitable getter method found for field intCol of class MyTableWithoutAccessors", ex.getMessage());

        ex = assertThrows(SQLException.class, () -> em.insert(conn, new MyTableWithNoMappedFields()));
        log.error(ex.getMessage());
        assertEquals("Entity class MyTableWithNoMappedFields has no @Column annotated fields", ex.getMessage());

        ex = assertThrows(SQLException.class, () -> em.insert(conn, new MyTableInvalidProperty()));
        log.error(ex.getMessage());
        assertEquals("Field timestampCol of entity class MyTableInvalidProperty is not a valid property", ex.getMessage());

        conn.rollback();
    }

}
