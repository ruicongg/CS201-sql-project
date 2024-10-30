package edu.smu.smusql;


import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EngineTest {
    private Engine engine;

    @BeforeEach
    void setUp() {
        engine = new Engine();
        // Create a test table for most tests
        engine.executeSQL("CREATE TABLE users (id, name, age, city)");
    }

    // CREATE TABLE tests
    @Test
    void testCreateTable_ValidSyntax_Success() {
        String result = engine.executeSQL("CREATE TABLE products (id, name, price)");
        assertEquals("Table products created", result);
    }

    @Test
    void testCreateTable_DuplicateTable_Error() {
        String result = engine.executeSQL("CREATE TABLE users (id, name)");
        assertEquals("ERROR: Table already exists", result);
    }

    @Test
    void testCreateTable_InvalidSyntax_Error() {
        String result = engine.executeSQL("CREATE users (id, name)");
        assertEquals("ERROR: Invalid CREATE TABLE syntax", result);
    }

    // INSERT tests
    @Test
    void testInsert_ValidSyntax_Success() {
        String result = engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        assertEquals("Row inserted into users", result);
    }

    @Test
    void testInsert_NonexistentTable_Error() {
        String result = engine.executeSQL("INSERT INTO nonexistent VALUES (1, John)");
        assertEquals("ERROR: Table not found", result);
    }

    @Test
    void testInsert_ColumnMismatch_Error() {
        String result = engine.executeSQL("INSERT INTO users VALUES (1, John, 25)");
        assertEquals("ERROR: Column count doesn't match value count", result);
    }

    // SELECT tests
    @Test
    void testSelect_NoWhereClause_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        String result = engine.executeSQL("SELECT * FROM users");
        assertTrue(result.contains("John") && result.contains("25") && result.contains("London"));
    }

    @Test
    void testSelect_WithEqualityCondition_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, Paris)");
        String result = engine.executeSQL("SELECT * FROM users WHERE name = John");
        assertTrue(result.contains("John") && !result.contains("Mary"));
    }

    @Test
    void testSelect_WithTwoConditions_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, London)");
        String result = engine.executeSQL("SELECT * FROM users WHERE city = London AND age > 20");
        assertTrue(result.contains("John") && result.contains("Mary"));
    }

    @Test
    void testSelect_NoResults_EmptyTable() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        String result = engine.executeSQL("SELECT * FROM users WHERE age > 50");
        assertEquals("id\tname\tage\tcity\n", result);
    }

    // UPDATE tests
    @Test
    void testUpdate_NoWhereClause_UpdatesAll() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, Paris)");
        String result = engine.executeSQL("UPDATE users SET city = Berlin");
        assertEquals("Table users updated. 2 rows affected.", result);
    }

    @Test
    void testUpdate_WithCondition_UpdatesMatching() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, Paris)");
        String result = engine.executeSQL("UPDATE users SET age = 26 WHERE name = John");
        assertEquals("Table users updated. 1 rows affected.", result);
    }

    @Test
    void testUpdate_NonexistentColumn_Error() {
        String result = engine.executeSQL("UPDATE users SET nonexistent = value");
        assertEquals("ERROR: Column not found", result);
    }

    // DELETE tests
    @Test
    void testDelete_WithCondition_DeletesMatching() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, Paris)");
        String result = engine.executeSQL("DELETE FROM users WHERE name = John");
        assertEquals("Rows deleted from users. 1 rows affected.", result);
    }

    @Test
    void testDelete_WithTwoConditions_DeletesMatching() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, London)");
        String result = engine.executeSQL("DELETE FROM users WHERE city = London AND age < 30");
        assertEquals("Rows deleted from users. 1 rows affected.", result);
    }

    // Boundary and Edge Cases
    @Test
    void testSelect_EmptyTable_ReturnsHeadersOnly() {
        String result = engine.executeSQL("SELECT * FROM users");
        assertEquals("id\tname\tage\tcity\n", result);
    }

    @Test
    void testUpdate_NoMatchingRows_ZeroAffected() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        String result = engine.executeSQL("UPDATE users SET age = 30 WHERE name = NonExistent");
        assertEquals("Table users updated. 0 rows affected.", result);
    }

    @Test
    void testDelete_NoMatchingRows_ZeroAffected() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        String result = engine.executeSQL("DELETE FROM users WHERE age > 100");
        assertEquals("Rows deleted from users. 0 rows affected.", result);
    }

    // Invalid Syntax Tests
    @Test
    void testSelect_InvalidSyntax_Error() {
        String result = engine.executeSQL("SELECT name FROM users");
        assertEquals("ERROR: Invalid SELECT syntax", result);
    }

    @Test
    void testUpdate_InvalidSyntax_Error() {
        String result = engine.executeSQL("UPDATE users city = London");
        assertEquals("ERROR: Invalid UPDATE syntax", result);
    }

    @Test
    void testDelete_InvalidSyntax_Error() {
        String result = engine.executeSQL("DELETE users");
        assertEquals("ERROR: Invalid DELETE syntax", result);
    }
} 