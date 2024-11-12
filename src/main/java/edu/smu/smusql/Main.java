package edu.smu.smusql;

import java.util.*;

import edu.smu.smusql.evaluator.EvaluationMode;
import edu.smu.smusql.evaluator.Evaluator;
import edu.smu.smusql.evaluator.SupportedQueries;
import edu.smu.smusql.lsm.*;

// @author ziyuanliu@smu.edu.sg

public class Main {
    /*
     *  Main method for accessing the command line interface of the database engine.
     *  MODIFICATION OF THIS FILE IS NOT RECOMMENDED!
     */
    static Engine dbEngine = new Engine();
    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("1. Enter 'exit' to exit");
            System.out.println("2. Enter 'evaluate' to evaluate the engine");
            System.out.println("3. Enter 'lsm' to test the lsm implementation");
            System.out.print("smusql> ");
            String query = scanner.nextLine();
            if (query.equalsIgnoreCase("exit")) {
                break;
            } else if (query.equalsIgnoreCase("evaluate")) {
                System.out.println("How would you like to evaluate the engine?");
                System.out.println("1. Enter 'random' for random percentages of queries");
                System.out.println("2. Enter 'interactive' to select each percentages of queries");
                System.out.print("smusql> ");

                EvaluationMode mode = EvaluationMode.valueOf(scanner.nextLine().toUpperCase());
                Evaluator evaluator = buildEvaluator(scanner, mode);

                long startTime = System.nanoTime();
//                evaluator.evaluate(numberOfQueries);
                long stopTime = System.nanoTime();
                long elapsedTime = stopTime - startTime;
                double elapsedTimeInSecond = (double) elapsedTime / 1_000_000_000;
                System.out.println("Time elapsed: " + elapsedTimeInSecond + " seconds");
                break;
            } else if (query.equalsIgnoreCase("lsm")) {
                LSMTreeTester.testLSM();
                break;
            }

            System.out.println(dbEngine.executeSQL(query));
        }
        scanner.close();
    }


    /*
     *  Below is the code for auto-evaluating your work.
     *  DO NOT CHANGE ANYTHING BELOW THIS LINE!
     */
    public static void autoEvaluate() {

        int numberOfQueries = 20000;

        // Create tables
        dbEngine.executeSQL("CREATE TABLE users (id, name, age, city)");
        dbEngine.executeSQL("CREATE TABLE products (id, name, price, category)");
        dbEngine.executeSQL("CREATE TABLE orders (id, user_id, product_id, quantity)");

        // Random data generator
        Random random = new Random();

        // Prepopulate the tables in preparation for evaluation
        prepopulateTables(random);

        // Loop to simulate millions of queries
        for (int i = 0; i < numberOfQueries; i++) {
            int queryType = random.nextInt(6);  // Randomly choose the type of query to execute

            switch (queryType) {
                case 0:  // INSERT query
                    insertRandomData(random);
                    break;
                case 1:  // SELECT query (simple)
                    selectRandomData(random);
                    break;
                case 2:  // UPDATE query
                    updateRandomData(random);
                    break;
                case 3:  // DELETE query
                    deleteRandomData(random);
                    break;
                case 4:  // Complex SELECT query with WHERE, AND, OR, >, <, LIKE
                    complexSelectQuery(random);
                    break;
                case 5:  // Complex UPDATE query with WHERE
                    complexUpdateQuery(random);
                    break;
            }

            // Print progress every 10,000 queries
            if (i % 10000 == 0){
                System.out.println("Processed " + i + " queries...");
            }
        }

        System.out.println("Finished processing " + numberOfQueries + " queries.");
    }

    private static void prepopulateTables(Random random) {
        System.out.println("Prepopulating users");
        // Insert initial users
        for (int i = 0; i < 50; i++) {
            String name = "User" + i;
            int age = 20 + (i % 41); // Ages between 20 and 60
            String city = getRandomCity(random);
            String insertCommand = String.format("INSERT INTO users VALUES (%d, '%s', %d, '%s')", i, name, age, city);
            dbEngine.executeSQL(insertCommand);
        }
        System.out.println("Prepopulating products");
        // Insert initial products
        for (int i = 0; i < 50; i++) {
            String productName = "Product" + i;
            double price = 10 + (i % 990); // Prices between $10 and $1000
            String category = getRandomCategory(random);
            String insertCommand = String.format("INSERT INTO products VALUES (%d, '%s', %.2f, '%s')", i, productName, price, category);
            dbEngine.executeSQL(insertCommand);
        }
        System.out.println("Prepopulating orders");
        // Insert initial orders
        for (int i = 0; i < 50; i++) {
            int user_id = random.nextInt(9999);
            int product_id = random.nextInt(9999);
            int quantity = random.nextInt(1, 100);
            String category = getRandomCategory(random);
            String insertCommand = String.format("INSERT INTO orders VALUES (%d, %d, %d, %d)", i, user_id, product_id, quantity);
            dbEngine.executeSQL(insertCommand);
        }
    }

    // Helper method to insert random data into users, products, or orders table
    private static void insertRandomData(Random random) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Insert into users table
                int id = random.nextInt(10000) + 10000;
                String name = "User" + id;
                int age = random.nextInt(60) + 20;
                String city = getRandomCity(random);
                String insertUserQuery = "INSERT INTO users VALUES (" + id + ", '" + name + "', " + age + ", '" + city + "')";
                dbEngine.executeSQL(insertUserQuery);
                break;
            case 1: // Insert into products table
                int productId = random.nextInt(1000) + 10000;
                String productName = "Product" + productId;
                double price = 50 + (random.nextDouble() * 1000);
                String category = getRandomCategory(random);
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

    // Helper method to randomly select data from tables
    private static void selectRandomData(Random random) {
        int tableChoice = random.nextInt(3);
        String selectQuery;
        switch (tableChoice) {
            case 0:
                selectQuery = "SELECT * FROM users";
                break;
            case 1:
                selectQuery = "SELECT * FROM products";
                break;
            case 2:
                selectQuery = "SELECT * FROM orders";
                break;
            default:
                selectQuery = "SELECT * FROM users";
        }
        dbEngine.executeSQL(selectQuery);
    }

    // Helper method to update random data in the tables
    private static void updateRandomData(Random random) {
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

    // Helper method to delete random data from tables
    private static void deleteRandomData(Random random) {
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

    // Helper method to execute a complex SELECT query with WHERE, AND, OR, >, <, LIKE
    private static void complexSelectQuery(Random random) {
        int tableChoice = random.nextInt(2);  // Complex queries only on users and products for now
        String complexSelectQuery;
        switch (tableChoice) {
            case 0: // Complex SELECT on users
                int minAge = random.nextInt(20) + 20;
                int maxAge = minAge + random.nextInt(30);
                String city = getRandomCity(random);
                complexSelectQuery = "SELECT * FROM users WHERE age > " + minAge + " AND age < " + maxAge;
                break;
            case 1: // Complex SELECT on products
                double minPrice = 50 + (random.nextDouble() * 200);
                double maxPrice = minPrice + random.nextDouble() * 500;
                complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice + " AND price < " + maxPrice;
                break;
            case 2: // Complex SELECT on products
                double minPrice2 = 50 + (random.nextDouble() * 200);
                String category = getRandomCategory(random);
                complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice2 + " AND category = " + category;
                break;
            default:
                complexSelectQuery = "SELECT * FROM users";
        }
        dbEngine.executeSQL(complexSelectQuery);
    }

    // Helper method to execute a complex UPDATE query with WHERE
    private static void complexUpdateQuery(Random random) {
        int tableChoice = random.nextInt(2);  // Complex updates only on users and products for now
        switch (tableChoice) {
            case 0: // Complex UPDATE on users
                int newAge = random.nextInt(60) + 20;
                String city = getRandomCity(random);
                String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE city = '" + city + "'";
                dbEngine.executeSQL(updateUserQuery);
                break;
            case 1: // Complex UPDATE on products
                double newPrice = 50 + (random.nextDouble() * 1000);
                String category = getRandomCategory(random);
                String updateProductQuery = "UPDATE products SET price = " + newPrice + " WHERE category = '" + category + "'";
                dbEngine.executeSQL(updateProductQuery);
                break;
        }
    }

    // Helper method to return a random city
    private static String getRandomCity(Random random) {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Boston", "Miami", "Seattle", "Austin", "Dallas", "Atlanta", "Denver"};
        return cities[random.nextInt(cities.length)];
    }

    // Helper method to return a random category for products
    private static String getRandomCategory(Random random) {
        String[] categories = {"Electronics", "Appliances", "Clothing", "Furniture", "Toys", "Sports", "Books", "Beauty", "Garden"};
        return categories[random.nextInt(categories.length)];
    }

    /**
     * Interactively builds an Evaluator instance by collecting percentages for each query type.
     *
     * @param scanner Scanner instance for reading user input.
     * @return Configured Evaluator instance or null if configuration failed.
     */
    private static Evaluator buildEvaluator(Scanner scanner, EvaluationMode mode) {
        double[] percentages = new double[SupportedQueries.values().length];

        if (mode.equals(EvaluationMode.RANDOM)) {
            getRandomPercentages(percentages);
        } else if (mode.equals(EvaluationMode.INTERACTIVE)) {
            getPercentageInput(scanner, percentages);
        } else {
            throw new IllegalArgumentException("Unknown evaluation mode: " + mode);
        }

        printPercentages(percentages);

        try {
            Evaluator evaluator = new Evaluator.Builder()
                    .insertPercentage(percentages[0])
                    .deletePercentage(percentages[1])
                    .updatePercentage(percentages[2])
                    .selectPercentage(percentages[3])
                    .build();
            System.out.println("Evaluator configured as: " + evaluator);
            return evaluator;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Prompts the user to enter a valid percentage for a given query type.
     *
     * @param scanner Scanner instance for reading user input.
     */
    private static void getPercentageInput(Scanner scanner, double[] percentages) {
        while (true) { // Loop until valid input is received
            double sum = 0.0;
            boolean validInput = true;

            // Iterate over each SupportedQuery to get user input
            for (int i = 0; i < SupportedQueries.values().length; i++) {
                double percentage = -1.0;

                // Prompt until a valid percentage is entered for the current query
                while (percentage < 0.0 || percentage > 100.0) {
                    System.out.print("Enter percentage for " + SupportedQueries.values()[i] + " queries: ");
                    String input = scanner.nextLine().trim();

                    if (input.equals("bye")) {
                        break;
                    }

                    try {
                        percentage = Double.parseDouble(input);

                        if (percentage < 0.0 || percentage > 100.0) {
                            System.out.println("Invalid input. Please enter a number between 0 and 100.");
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid input. Please enter a valid number.");
                        percentage = -1.0; // Reset to force re-entry
                    }
                }

                percentages[i] = percentage;
                sum += percentage;
            }

            // Check if the total sum of percentages is exactly 100%
            if (Math.abs(sum - 100.0) < 1e-6) { // Using a small epsilon for floating-point comparison
                break; // Valid input; exit the loop
            } else {
                System.out.println("\nThe total sum of percentages is " + sum + "%, which does not equal 100%.");
                System.out.println("Please re-enter all percentages.\n");
                // Optionally, reset the percentages array
                Arrays.fill(percentages, 0.0);
            }
        }
    }

    /**
     * Generates an array of random percentages that sum up to 100.
     *
     * @param percentages The percentage for each query types.
     * @return An array of random percentages.
     */
    private static void getRandomPercentages(double[] percentages) {
        if (percentages == null || percentages.length != SupportedQueries.values().length) {
            throw new IllegalArgumentException("Input array must be non-null and of size 4.");
        }

        Random random = new Random();

        // Generate three unique random numbers between 1 and 99
        int[] cuts = new int[3];
        for (int i = 0; i < 3; i++) {
            cuts[i] = random.nextInt(99) + 1; // [1, 99]
        }

        // Sort the cut points
        Arrays.sort(cuts);

        // Calculate the differences to get the percentages
        percentages[0] = cuts[0];
        percentages[1] = cuts[1] - cuts[0];
        percentages[2] = cuts[2] - cuts[1];
        percentages[3] = 100 - cuts[2];
    }

    private static void printPercentages(double[] percentages) {
        for (int i = 0; i < percentages.length; i++) {
            System.out.println(SupportedQueries.values()[i] + ":" + percentages[i] + "%");
        }
    }
}