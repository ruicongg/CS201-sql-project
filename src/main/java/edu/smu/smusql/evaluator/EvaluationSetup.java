package edu.smu.smusql.evaluator;

import edu.smu.smusql.Engine;

import java.util.Random;

public class EvaluationSetup {

    private final Engine dbEngine;
    private final Random random = new Random();

    public EvaluationSetup(Engine dbEngine) {
        this.dbEngine = dbEngine;
    }

    public void setup(long userRows, long productRows, long orderRows) {
        System.out.println("Setting up the evaluation environment...");
        setupUserTable(userRows);
        setupOrderTable(orderRows);
        setupProductTable(productRows);
    }

    private void setupUserTable(long userRows) {
        // Create table
        System.out.println("Prepopulating users");
        dbEngine.executeSQL("CREATE TABLE users (id, name, age, city)");
        for (int i = 0; i < userRows; i++) {
            String name = "User" + i;
            int age = 20 + (i % 41); // Prices between $10 and $1000
            String city = AttributeRandomizer.getRandomCity(random);
            String insertCommand = String.format("INSERT INTO users VALUES (%d, '%s', %.2f, '%s')", i, name, age, city);
            dbEngine.executeSQL(insertCommand);
        }
    }

    private void setupProductTable(long productRows) {
        System.out.println("Prepopulating products");
        dbEngine.executeSQL("CREATE TABLE products (id, name, price, category)");

        for (int i = 0; i < productRows; i++) {
            String productName = "Product" + i;
            double price = 10 + (i % 990); // Prices between $10 and $1000
            String category = AttributeRandomizer.getRandomCategory(random);
            String insertCommand = String.format("INSERT INTO products VALUES (%d, '%s', %.2f, '%s')", i, productName, price, category);
            dbEngine.executeSQL(insertCommand);
        }
    }

    private void setupOrderTable(long orderRows) {
        System.out.println("Prepopulating orders");
        dbEngine.executeSQL("CREATE TABLE orders (id, user_id, product_id, quantity)");

        // Insert initial orders
        for (int i = 0; i < orderRows; i++) {
            int user_id = random.nextInt(9999);
            int product_id = random.nextInt(9999);
            int quantity = random.nextInt(1, 100);
            String insertCommand = String.format("INSERT INTO orders VALUES (%d, %d, %d, %d)", i, user_id, product_id, quantity);
            dbEngine.executeSQL(insertCommand);
        }
    }
}
