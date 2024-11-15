package edu.smu.smusql.evaluator;

import edu.smu.smusql.Engine;

import java.util.Random;

/**
 * Evaluator is responsible for evaluating the performance of a database by executing
 * a mix of different types of SQL queries (INSERT, DELETE, UPDATE, SELECT) based on
 * specified percentages.
 * @author gav
 */
public class Evaluator {

    private final Metrics metrics = new Metrics();
    private final Random random = new Random();
    private final Engine dbEngine;
    private final double insertPercentage;
    private final double deletePercentage;
    private final double updatePercentage;
    private final double selectPercentage;

    private double simpleInsertSpeed = 0.0;
    private double simpleUpdateSpeed = 0.0;
    private double simpleDeleteSpeed = 0.0;
    private double simpleSelectSpeed = 0.0;
    private double complexSelectSpeed = 0.0;
    private double complexUpdateSpeed = 0.0;

    /**
     * Private constructor to enforce object creation through the Builder.
     *
     * @param builder The Builder instance containing the configuration.
     */
    private Evaluator(Builder builder) {
        this.insertPercentage = builder.insertPercentage;
        this.deletePercentage = builder.deletePercentage;
        this.updatePercentage = builder.updatePercentage;
        this.selectPercentage = builder.selectPercentage;
        this.dbEngine = builder.dbEngine;
    }

    // Getters for each query type percentage

    public double getInsertPercentage() {
        return insertPercentage;
    }

    public double getDeletePercentage() {
        return deletePercentage;
    }

    public double getUpdatePercentage() {
        return updatePercentage;
    }

    public double getSelectPercentage() {
        return selectPercentage;
    }

    /**
     * Builder class for Evaluator. Allows setting percentages for different query types.
     */
    public static class Builder {
        private double insertPercentage = 0.0;
        private double deletePercentage = 0.0;
        private double updatePercentage = 0.0;
        private double selectPercentage = 0.0;
        private Engine dbEngine = null;

        /**
         * Sets the percentage for INSERT queries.
         *
         * @param insertPercentage Percentage of INSERT queries (0-100).
         * @return The Builder instance.
         */
        public Builder insertPercentage(double insertPercentage) {
            this.insertPercentage = insertPercentage;
            return this;
        }

        /**
         * Sets the percentage for DELETE queries.
         *
         * @param deletePercentage Percentage of DELETE queries (0-100).
         * @return The Builder instance.
         */
        public Builder deletePercentage(double deletePercentage) {
            this.deletePercentage = deletePercentage;
            return this;
        }

        /**
         * Sets the percentage for UPDATE queries.
         *
         * @param updatePercentage Percentage of UPDATE queries (0-100).
         * @return The Builder instance.
         */
        public Builder updatePercentage(double updatePercentage) {
            this.updatePercentage = updatePercentage;
            return this;
        }

        /**
         * Sets the percentage for SELECT queries.
         *
         * @param selectPercentage Percentage of SELECT queries (0-100).
         * @return The Builder instance.
         */
        public Builder selectPercentage(double selectPercentage) {
            this.selectPercentage = selectPercentage;
            return this;
        }

        /**
         * Sets dbEngine for evaluation.
         *
         * @param dbEngine SQL engine to test.
         * @return The Builder instance.
         */
        public Builder dbEngine(Engine dbEngine) {
            this.dbEngine = dbEngine;
            return this;
        }

        /**
         * Validates the percentages and builds an Evaluator instance.
         *
         * @return A new Evaluator instance.
         * @throws IllegalArgumentException If the total percentage does not equal 100.
         */
        public Evaluator build() {
            double total = insertPercentage + deletePercentage + updatePercentage + selectPercentage;
            if (Math.abs(total - 100.0) > 0.0001) { // Allowing a tiny margin for floating point precision
                throw new IllegalArgumentException(
                        String.format("Total percentage must be 100. Current total: %.2f", total));
            }

            if (insertPercentage < 0 || deletePercentage < 0 || updatePercentage < 0 || selectPercentage < 0) {
                throw new IllegalArgumentException("No negative values allowed.");
            }

            divideBy100();

            return new Evaluator(this);
        }

        private void divideBy100() {
            this.selectPercentage /= 100;
            this.insertPercentage /= 100;
            this.deletePercentage /= 100;
            this.updatePercentage /= 100;
        }
    }

    @Override
    public String toString() {
        return "Evaluator{" +
                "insertPercentage=" + insertPercentage +
                ", deletePercentage=" + deletePercentage +
                ", updatePercentage=" + updatePercentage +
                ", selectPercentage=" + selectPercentage +
                '}';
    }

    public void evaluate(double complexPercentage, long numQueries) {
        complexSelectQuery(random, Math.round(complexPercentage * selectPercentage * numQueries));
        complexUpdateQuery(random, Math.round(complexPercentage * updatePercentage * numQueries));

        insertRandomData(random, Math.round((1 - complexPercentage) * insertPercentage * numQueries));
        deleteRandomData(random, Math.round(deletePercentage * numQueries));
        updateRandomData(random, Math.round((1 - complexPercentage) * updatePercentage * numQueries));
        selectRandomData(random, Math.round(selectPercentage * numQueries));

        printMetrics();
    }

    public void evaluateRandomly(double complexPercentage, long numQueries) {
        //cycle through each type of query 
        //Determine the number of queries for each type
        long numComplexSelects = Math.round(complexPercentage * selectPercentage * numQueries);
        long numComplexUpdates = Math.round(complexPercentage * updatePercentage * numQueries);
        long numInserts = Math.round((1 - complexPercentage) * insertPercentage * numQueries);
        long numDeletes = Math.round(deletePercentage * numQueries);
        long numUpdates = Math.round((1 - complexPercentage) * updatePercentage * numQueries);
        long numSelects = Math.round(selectPercentage * numQueries);

        Random randomQuery = new Random();

        for (long i = 0; i < numQueries; i++) {
            int queryType = randomQuery.nextInt(6); //0 to 5 for the 6 query types
    
            switch (queryType) {
                case 0: //complex select
                    if (numComplexSelects > 0) {
                        complexSelectQuery(random, 1);
                        numComplexSelects--;
                    } else {
                        i--; 
                    }
                    break;
    
                case 1: //complex update
                    if (numComplexUpdates > 0) {
                        complexUpdateQuery(random, 1);
                        numComplexUpdates--;
                    } else {
                        i--;
                    }
                    break;
    
                case 2: //insert
                    if (numInserts > 0) {
                        insertRandomData(random, 1);
                        numInserts--;
                    } else {
                        i--;
                    }
                    break;
    
                case 3: //delete
                    if (numDeletes > 0) {
                        deleteRandomData(random, 1);
                        numDeletes--;
                    } else {
                        i--;
                    }
                    break;
    
                case 4: //update
                    if (numUpdates > 0) {
                        updateRandomData(random, 1);
                        numUpdates--;
                    } else {
                        i--;
                    }
                    break;
    
                case 5: //simple select
                    if (numSelects > 0) {
                        selectRandomData(random, 1);
                        numSelects--;
                    } else {
                        i--;
                    }
                    break;
            }
        }
    
        printMetrics();
    }

    // Helper method to insert random data into users, products, or orders table
    private void insertRandomData(Random random, long insertQueries) {
        System.out.println("Executing " + insertQueries + " simple insert queries...");
        long startTime = System.nanoTime();
        for (long i = 0; i < insertQueries; i++) {

            if (i % 1000 == 0) {
                System.out.println("Ran " + i + " simple insert queries...");
            }

            int tableChoice = random.nextInt(3);
            switch (tableChoice) {
                case 0: // Insert into users table
                    int id = random.nextInt(10000) + 10000;
                    String name = "User" + id;
                    int age = random.nextInt(60) + 20;
                    String city = AttributeRandomizer.getRandomCity(random);
                    String insertUserQuery = "INSERT INTO users VALUES (" + id + ", '" + name + "', " + age + ", '" + city + "')";
                    dbEngine.executeSQL(insertUserQuery);
                    break;
                case 1: // Insert into products table
                    int productId = random.nextInt(1000) + 10000;
                    String productName = "Product" + productId;
                    double price = 50 + (random.nextDouble() * 1000);
                    String category = AttributeRandomizer.getRandomCategory(random);
                    String insertProductQuery = "INSERT INTO products VALUES (" + productId + ", '" + productName + "', " + price + ", '" + category + "')";
                    dbEngine.executeSQL(insertProductQuery);
                    break;
                case 2: // Insert into orders table
                    int orderId = random.nextInt(10000) + 1;
                    int userId = random.nextInt(10000) + 1;
                    int productIdRef = random.nextInt(1000) + 1;
                    int quantity = random.nextInt(10) + 1;
                    String insertOrderQuery = "INSERT INTO orders VALUES (" + orderId + ", " + userId + ", " + productIdRef + ", " + quantity + ")";
                    dbEngine.executeSQL(insertOrderQuery);
                    break;
            }
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        this.simpleInsertSpeed += (double) (((double)elapsedTime / 1_000_000)/insertQueries);
    }

    // Helper method to randomly select data from tables
    private void selectRandomData(Random random,  long selectQueries) {
        System.out.println("Executing " + selectQueries + " simple select queries...");
        long startTime = System.nanoTime();
        for (long i = 0; i < selectQueries; i++) {

            if (i % 1000 == 0) {
                System.out.println("Ran " + i + " simple select queries...");
            }

            int tableChoice = random.nextInt(3);
            String selectQuery = switch (tableChoice) {
                case 0 -> "SELECT * FROM users";
                case 1 -> "SELECT * FROM products";
                case 2 -> "SELECT * FROM orders";
                default -> "SELECT * FROM users";
            };
            dbEngine.executeSQL(selectQuery);
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        this.simpleSelectSpeed += (double) (((double)elapsedTime / 1_000_000)/selectQueries);
    }

    // Helper method to update random data in the tables
    private void updateRandomData(Random random,  long updateQueries) {
        System.out.println("Executing " + updateQueries + " simple update queries...");
        long startTime = System.nanoTime();
        for (long i = 0; i < updateQueries; i++) {

            if (i % 1000 == 0) {
                System.out.println("Ran " + i + " simple update queries...");
            }

            int tableChoice = random.nextInt(3);
            switch (tableChoice) {
                case 0: // Update users table
                    int id = random.nextInt(10000) + 1;
                    int newAge = random.nextInt(60) + 20;
                    String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE id = " + id;
                    dbEngine.executeSQL(updateUserQuery);
                    break;
                case 1: // Update products table
                    int productId = random.nextInt(1000) + 1;
                    double newPrice = 50 + (random.nextDouble() * 1000);
                    String updateProductQuery = "UPDATE products SET price = " + newPrice + " WHERE id = " + productId;
                    dbEngine.executeSQL(updateProductQuery);
                    break;
                case 2: // Update orders table
                    int orderId = random.nextInt(10000) + 1;
                    int newQuantity = random.nextInt(10) + 1;
                    String updateOrderQuery = "UPDATE orders SET quantity = " + newQuantity + " WHERE id = " + orderId;
                    dbEngine.executeSQL(updateOrderQuery);
                    break;
            }
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        this.simpleUpdateSpeed += (double) (((double)elapsedTime / 1_000_000)/updateQueries);
    }

    // Helper method to delete random data from tables
    private void deleteRandomData(Random random, long deleteQueries) {
        System.out.println("Executing " + deleteQueries + " simple delete queries...");
        long startTime = System.nanoTime();
        for (long i = 0; i < deleteQueries; i++) {

            if (i % 1000 == 0) {
                System.out.println("Ran " + i + " simple delete queries...");
            }

            int tableChoice = random.nextInt(3);
            switch (tableChoice) {
                case 0: // Delete from users table
                    int userId = random.nextInt(10000) + 1;
                    String deleteUserQuery = "DELETE FROM users WHERE id = " + userId;
                    dbEngine.executeSQL(deleteUserQuery);
                    break;
                case 1: // Delete from products table
                    int productId = random.nextInt(1000) + 1;
                    String deleteProductQuery = "DELETE FROM products WHERE id = " + productId;
                    dbEngine.executeSQL(deleteProductQuery);
                    break;
                case 2: // Delete from orders table
                    int orderId = random.nextInt(10000) + 1;
                    String deleteOrderQuery = "DELETE FROM orders WHERE id = " + orderId;
                    dbEngine.executeSQL(deleteOrderQuery);
                    break;
            }
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        this.simpleDeleteSpeed += (double) (((double)elapsedTime / 1_000_000)/deleteQueries);
    }

    // Helper method to execute a complex UPDATE query with WHERE
    private void complexUpdateQuery(Random random, long complexUpdateQueries) {
        System.out.println("Executing " + complexUpdateQueries + " complex update queries...");
        long startTime = System.nanoTime();
        for (long i = 0; i < complexUpdateQueries; i++) {

            if (i % 1000 == 0) {
                System.out.println("Ran " + i + " complex update queries...");
            }

            int tableChoice = random.nextInt(2);  // Complex updates only on users and products for now
            switch (tableChoice) {
                case 0: // Complex UPDATE on users
                    int newAge = random.nextInt(60) + 20;
                    String city = AttributeRandomizer.getRandomCity(random);
                    String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE city = '" + city + "'";
                    dbEngine.executeSQL(updateUserQuery);
                    break;
                case 1: // Complex UPDATE on products
                    double newPrice = 50 + (random.nextDouble() * 1000);
                    String category = AttributeRandomizer.getRandomCategory(random);
                    String updateProductQuery = "UPDATE products SET price = " + newPrice + " WHERE category = '" + category + "'";
                    dbEngine.executeSQL(updateProductQuery);
                    break;
            }
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        this.complexUpdateSpeed += (double) (((double)elapsedTime / 1_000_000)/complexUpdateQueries);
    }

    // Helper method to execute a complex SELECT query with WHERE, AND, OR, >, <, LIKE
    private void complexSelectQuery(Random random, long complexSelectQueries) {
        System.out.println("Executing " + complexSelectQueries + " complex select queries...");
        long startTime = System.nanoTime();
        for (long i = 0; i < complexSelectQueries; i++) {

            if (i % 1000 == 0) {
                System.out.println("Ran " + i + " complex select queries...");
            }

            int tableChoice = random.nextInt(2);  // Complex queries only on users and products for now
            String complexSelectQuery;
            switch (tableChoice) {
                case 0: // Complex SELECT on users
                    int minAge = random.nextInt(20) + 20;
                    int maxAge = minAge + random.nextInt(30);
                    String city = AttributeRandomizer.getRandomCity(random);
                    complexSelectQuery = "SELECT * FROM users WHERE age > " + minAge + " AND age < " + maxAge;
                    break;
                case 1: // Complex SELECT on products
                    double minPrice = 50 + (random.nextDouble() * 200);
                    double maxPrice = minPrice + random.nextDouble() * 500;
                    complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice + " AND price < " + maxPrice;
                    break;
                case 2: // Complex SELECT on products
                    double minPrice2 = 50 + (random.nextDouble() * 200);
                    String category = AttributeRandomizer.getRandomCategory(random);
                    complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice2 + " AND category = " + category;
                    break;
                default:
                    complexSelectQuery = "SELECT * FROM users";
            }
            dbEngine.executeSQL(complexSelectQuery);
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        this.complexSelectSpeed += (double) (((double)elapsedTime / 1_000_000)/complexSelectQueries);
    }


    private void printMetrics() {
        System.out.println("Simple Select Speed: " + simpleSelectSpeed + " milliseconds per query.");
        System.out.println("Simple Insert Speed: " + simpleInsertSpeed + " milliseconds per query.");
        System.out.println("Simple Update Speed: " + simpleUpdateSpeed + " milliseconds per query.");
        System.out.println("Simple Delete Speed: " + simpleDeleteSpeed + " milliseconds per query.");
        System.out.println("Complex Select Speed: " + complexSelectSpeed + " milliseconds per query.");
        System.out.println("Complex Update Speed: " + complexUpdateSpeed + " milliseconds per query.");
        System.out.println("Memory usage: " + metrics.getUsedMemory() + " MB.");
    }
}
