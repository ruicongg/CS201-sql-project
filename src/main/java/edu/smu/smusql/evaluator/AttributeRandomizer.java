package edu.smu.smusql.evaluator;

import java.util.Random;

public final class AttributeRandomizer {

    private AttributeRandomizer() {
        throw new IllegalStateException("Utility class");
    }

    // Helper method to return a random city
    public static String getRandomCity(Random random) {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Boston", "Miami", "Seattle", "Austin", "Dallas", "Atlanta", "Denver"};
        return cities[random.nextInt(cities.length)];
    }

    // Helper method to return a random category for products
    public static String getRandomCategory(Random random) {
        String[] categories = {"Electronics", "Appliances", "Clothing", "Furniture", "Toys", "Sports", "Books", "Beauty", "Garden"};
        return categories[random.nextInt(categories.length)];
    }
}
