package edu.smu.smusql;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;


// These test cases only test the functionality as per required in the handout.
// Cases not tested:
// - More than 2 where conditions
// - Extra parameters in query
// - Empty strings in where conditions

public class    EngineTest {
    private Engine engine;
    private static final long TEST_ITERATIONS = 100;

    @BeforeEach
    void setUp() {
        engine = new Engine();
        // Create a test table for most tests
        engine.executeSQL("CREATE TABLE users (id, name, age, city)");
    }

    @AfterEach
    void tearDown() {
        engine.executeSQL("DROP TABLE users");
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
    void testSelect_InvalidSyntax_Error() {
        String result = engine.executeSQL("SELECT name FROM users");
        assertEquals("ERROR: Invalid SELECT syntax", result);
    }

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
    void testSelectWithGreaterThanCondition_NoResults_EmptyTable() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        String result = engine.executeSQL("SELECT * FROM users WHERE age > 50");
        assertEquals("id\tname\tage\tcity\n", result);
    }

    @Test
    void testSelectWithLessThanCondition_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        String result = engine.executeSQL("SELECT * FROM users WHERE age < 30");
        assertTrue(result.contains("John") && result.contains("25") && result.contains("London"));
    }

    @Test
    void testSelect_WithBoundaryValues_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 0, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 100, Paris)");
        String result = engine.executeSQL("SELECT * FROM users WHERE age >= 0 AND age <= 100");
        assertTrue(result.contains("John") && result.contains("Mary"));
    }

    @Test
    void testSelect_WithGreaterThanEqualCondition_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, Paris)");
        String result = engine.executeSQL("SELECT * FROM users WHERE age >= 30");
        assertTrue(result.contains("Mary") && !result.contains("John"));
    }

    @Test
    void testSelect_WithLessThanEqualCondition_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, Paris)");
        String result = engine.executeSQL("SELECT * FROM users WHERE age <= 25");
        assertTrue(result.contains("John") && !result.contains("Mary"));
    }

    @Test
    void testSelect_WithEqualCondition_NumericValue_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 25, Paris)");
        String result = engine.executeSQL("SELECT * FROM users WHERE age = 25");
        assertTrue(result.contains("John") && result.contains("Mary"));
    }

    @Test
    void testSelect_WithMultipleConditions_MixedOperators_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, London)");
        engine.executeSQL("INSERT INTO users VALUES (3, Bob, 35, Paris)");
        String result = engine.executeSQL("SELECT * FROM users WHERE age >= 25 AND age <= 30 AND city = London");
        assertTrue(result.contains("John") && result.contains("Mary") && !result.contains("Bob"));
    }

    
    // UPDATE tests
    @Test
    void testUpdate_InvalidSyntax_Error() {
        String result = engine.executeSQL("UPDATE users city = London");
        assertEquals("ERROR: Invalid UPDATE syntax", result);
    }

    @Test
    void testUpdate_WithCondition_UpdatesMatching() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, Paris)");
        String result = engine.executeSQL("UPDATE users SET age = 26 WHERE name = John");
        assertEquals("Table users updated. 1 rows affected.", result);
    }

    @Test
    void testUpdate_NoMatchingRows_ZeroAffected() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        String result = engine.executeSQL("UPDATE users SET age = 30 WHERE name = NonExistent");
        assertEquals("Table users updated. 0 rows affected.", result);
    }

    @Test
    void testUpdate_NonexistentColumn_Error() {
        String result = engine.executeSQL("UPDATE users SET nonexistent = value");
        assertEquals("ERROR: Column not found", result);
    }

    @Test
    void testUpdate_WithGreaterThanEqualCondition_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, Paris)");
        String result = engine.executeSQL("UPDATE users SET city = Berlin WHERE age >= 30");
        assertEquals("Table users updated. 1 rows affected.", result);
    }

    @Test
    void testUpdate_WithLessThanEqualCondition_Success() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        engine.executeSQL("INSERT INTO users VALUES (2, Mary, 30, Paris)");
        String result = engine.executeSQL("UPDATE users SET city = Berlin WHERE age <= 25");
        assertEquals("Table users updated. 1 rows affected.", result);
    }

    // DELETE tests
    @Test
    void testDelete_InvalidSyntax_Error() {
        String result = engine.executeSQL("DELETE users");
        assertEquals("ERROR: Invalid DELETE syntax", result);
    }

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

    @Test
    void testDelete_NoMatchingRows_ZeroAffected() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");
        String result = engine.executeSQL("DELETE FROM users WHERE age > 100");
        assertEquals("Rows deleted from users. 0 rows affected.", result);
    }

    @Test
    void testSelectCache_HitSuccess() {
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            engine.executeSQL("INSERT INTO users VALUES (" + i + ", John, 25, London)");
        }

        // First query - should miss cache
        long start1 = System.currentTimeMillis();
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            engine.executeSQL("SELECT * FROM users WHERE age = 25");
        }
        long firstRun = System.currentTimeMillis() - start1;

        // Second query - should hit cache
        long start2 = System.currentTimeMillis();
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            engine.executeSQL("SELECT * FROM users WHERE age = 25");
        }
        long secondRun = System.currentTimeMillis() - start2;

        assertTrue(firstRun > secondRun,
                "First run should be slower than seocnd run");
    }

    // this test is to prevent stale data (cuz we hold onto old cache copy)
    @Test
    void testSelectCache_InvalidationOnUpdate() {
        engine.executeSQL("INSERT INTO users VALUES (1, John, 25, London)");

        // Cache the select result
        String result1 = engine.executeSQL("SELECT * FROM users WHERE age = 25");

        // Update data
        engine.executeSQL("UPDATE users SET age = 26 WHERE name = John");

        // Should get new result after update
        String result2 = engine.executeSQL("SELECT * FROM users WHERE age = 25");

        assertNotEquals(result1, result2);
    }
} 