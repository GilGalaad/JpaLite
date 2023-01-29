package org.jpalite;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Slf4j
public abstract class TestSession {

    protected static Connection conn;
    protected final EntityManager em = new EntityManager();

    @BeforeAll
    protected static void init() throws SQLException {
        log.info("Connecting to database");
        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", "localhost", 5432, "postgres");
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", "postgres");
        jdbcProps.setProperty("prepareThreshold", "0");
        conn = DriverManager.getConnection(jdbcUrl, jdbcProps);
        conn.setAutoCommit(false);
        dropSchema();
        createSchema();
    }

    @AfterAll
    protected static void cleanup() throws SQLException {
        if (conn != null) {
            dropSchema();
            createSchema();
            log.info("Closing database connection");
            conn.close();
        }
    }

    @BeforeEach
    @AfterEach
    protected void dropTable() throws SQLException {
        execute("DROP TABLE IF EXISTS test_table");
    }

    protected static void execute(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
        conn.commit();
    }

    protected static void createSchema() throws SQLException {
        execute("CREATE SCHEMA IF NOT EXISTS public");
    }

    protected static void dropSchema() throws SQLException {
        execute("DROP SCHEMA IF EXISTS public CASCADE");
    }

}
