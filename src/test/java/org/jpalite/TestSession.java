package org.jpalite;

import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Log4j2
public abstract class TestSession {

    protected static Connection conn;
    protected final EntityManager em = new EntityManager();

    @BeforeAll
    protected static void init() throws SQLException {
        log.debug("Connecting to database");
        String jdbcUrl = "jdbc:h2:mem:test_mem";
        conn = DriverManager.getConnection(jdbcUrl);
        conn.setAutoCommit(false);
        dropSchema();
        createSchema();
    }

    @AfterAll
    protected static void cleanup() throws SQLException {
        if (conn != null) {
            dropSchema();
            createSchema();
            log.debug("Closing database connection");
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
        execute("CREATE SCHEMA IF NOT EXISTS jpalite");
    }

    protected static void dropSchema() throws SQLException {
        execute("DROP SCHEMA IF EXISTS jpalite CASCADE");
    }

}
