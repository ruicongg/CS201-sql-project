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
    private final double insertPercentage;
    private final double deletePercentage;
    private final double updatePercentage;
    private final double selectPercentage;

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

            return new Evaluator(this);
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

    public void evaluate(Engine engine, int complexPercentage, long numQueries) {

    }
//
//    // Helper method to execute a complex UPDATE query with WHERE
//    private static void complexUpdateQuery(Random random) {
//        int tableChoice = random.nextInt(2);  // Complex updates only on users and products for now
//        switch (tableChoice) {
//            case 0: // Complex UPDATE on users
//                int newAge = random.nextInt(60) + 20;
//                String city = getRandomCity(random);
//                String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE city = '" + city + "'";
//                dbEngine.executeSQL(updateUserQuery);
//                break;
//            case 1: // Complex UPDATE on products
//                double newPrice = 50 + (random.nextDouble() * 1000);
//                String category = getRandomCategory(random);
//                String updateProductQuery = "UPDATE products SET price = " + newPrice + " WHERE category = '" + category + "'";
//                dbEngine.executeSQL(updateProductQuery);
//                break;
//        }
//    }
//
//    // Helper method to execute a complex SELECT query with WHERE, AND, OR, >, <, LIKE
//    private static void complexSelectQuery(Random random) {
//        int tableChoice = random.nextInt(2);  // Complex queries only on users and products for now
//        String complexSelectQuery;
//        switch (tableChoice) {
//            case 0: // Complex SELECT on users
//                int minAge = random.nextInt(20) + 20;
//                int maxAge = minAge + random.nextInt(30);
//                String city = getRandomCity(random);
//                complexSelectQuery = "SELECT * FROM users WHERE age > " + minAge + " AND age < " + maxAge;
//                break;
//            case 1: // Complex SELECT on products
//                double minPrice = 50 + (random.nextDouble() * 200);
//                double maxPrice = minPrice + random.nextDouble() * 500;
//                complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice + " AND price < " + maxPrice;
//                break;
//            case 2: // Complex SELECT on products
//                double minPrice2 = 50 + (random.nextDouble() * 200);
//                String category = getRandomCategory(random);
//                complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice2 + " AND category = " + category;
//                break;
//            default:
//                complexSelectQuery = "SELECT * FROM users";
//        }
//        dbEngine.executeSQL(complexSelectQuery);
//    }
//
//
//    // Helper method to insert random data into users, products, or orders table
//    private static void insertRandomData(Random random) {
//        int tableChoice = random.nextInt(3);
//        switch (tableChoice) {
//            case 0: // Insert into users table
//                int id = random.nextInt(10000) + 10000;
//                String name = "User" + id;
//                int age = random.nextInt(60) + 20;
//                String city = getRandomCity(random);
//                String insertUserQuery = "INSERT INTO users VALUES (" + id + ", '" + name + "', " + age + ", '" + city + "')";
//                dbEngine.executeSQL(insertUserQuery);
//                break;
//            case 1: // Insert into products table
//                int productId = random.nextInt(1000) + 10000;
//                String productName = "Product" + productId;
//                double price = 50 + (random.nextDouble() * 1000);
//                String category = getRandomCategory(random);
//                String insertProductQuery = "INSERT INTO products VALUES (" + productId + ", '" + productName + "', " + price + ", '" + category + "')";
//                dbEngine.executeSQL(insertProductQuery);
//                break;
//            case 2: // Insert into orders table
//                int orderId = random.nextInt(10000) + 1;
//                int userId = random.nextInt(10000) + 1;
//                int productIdRef = random.nextInt(1000) + 1;
//                int quantity = random.nextInt(10) + 1;
//                String insertOrderQuery = "INSERT INTO orders VALUES (" + orderId + ", " + userId + ", " + productIdRef + ", " + quantity + ")";
//                dbEngine.executeSQL(insertOrderQuery);
//                break;
//        }
//    }
//
//    // Helper method to randomly select data from tables
//    private static void selectRandomData(Random random) {
//        int tableChoice = random.nextInt(3);
//        String selectQuery;
//        switch (tableChoice) {
//            case 0:
//                selectQuery = "SELECT * FROM users";
//                break;
//            case 1:
//                selectQuery = "SELECT * FROM products";
//                break;
//            case 2:
//                selectQuery = "SELECT * FROM orders";
//                break;
//            default:
//                selectQuery = "SELECT * FROM users";
//        }
//        dbEngine.executeSQL(selectQuery);
//    }
//
//    // Helper method to update random data in the tables
//    private static void updateRandomData(Random random) {
//        int tableChoice = random.nextInt(3);
//        switch (tableChoice) {
//            case 0: // Update users table
//                int id = random.nextInt(10000) + 1;
//                int newAge = random.nextInt(60) + 20;
//                String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE id = " + id;
//                dbEngine.executeSQL(updateUserQuery);
//                break;
//            case 1: // Update products table
//                int productId = random.nextInt(1000) + 1;
//                double newPrice = 50 + (random.nextDouble() * 1000);
//                String updateProductQuery = "UPDATE products SET price = " + newPrice + " WHERE id = " + productId;
//                dbEngine.executeSQL(updateProductQuery);
//                break;
//            case 2: // Update orders table
//                int orderId = random.nextInt(10000) + 1;
//                int newQuantity = random.nextInt(10) + 1;
//                String updateOrderQuery = "UPDATE orders SET quantity = " + newQuantity + " WHERE id = " + orderId;
//                dbEngine.executeSQL(updateOrderQuery);
//                break;
//        }
//    }
//
//    // Helper method to delete random data from tables
//    private static void deleteRandomData(Random random) {
//        int tableChoice = random.nextInt(3);
//        switch (tableChoice) {
//            case 0: // Delete from users table
//                int userId = random.nextInt(10000) + 1;
//                String deleteUserQuery = "DELETE FROM users WHERE id = " + userId;
//                dbEngine.executeSQL(deleteUserQuery);
//                break;
//            case 1: // Delete from products table
//                int productId = random.nextInt(1000) + 1;
//                String deleteProductQuery = "DELETE FROM products WHERE id = " + productId;
//                dbEngine.executeSQL(deleteProductQuery);
//                break;
//            case 2: // Delete from orders table
//                int orderId = random.nextInt(10000) + 1;
//                String deleteOrderQuery = "DELETE FROM orders WHERE id = " + orderId;
//                dbEngine.executeSQL(deleteOrderQuery);
//                break;
//        }
//    }

    private void printMetrics() {
        System.out.println("Throughput: " + metrics.getThroughput() + " queries/sec.");
        System.out.println("Average Latency: " + metrics.getAverageLatency() + " ms.");
        System.out.println("Min Latency: " + metrics.getMinLatency() + " ms.");
        System.out.println("Max Latency: " + metrics.getMaxLatency() + " ms.");
    }
}
