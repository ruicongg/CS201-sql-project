package edu.smu.smusql.lsm;

import java.util.*;

public class LSMTreeTester {
    private static final int LIMIT = 20; 

    public static void testLSM() {
        Random random = new Random();

        LSMTree tree = new LSMTree();

        for (int i = 0; i < LIMIT; i++) {
            String name = "User" + i;
            int age = 20 + (i % 41); // Ages between 20 and 60
            String city = getRandomCity(random);

            List<String> values = new ArrayList<>();
            values.add(String.valueOf(age));
            values.add(city);

            tree.add(name, values);
            tree.printTree();
        }
    }

    // Helper method to return a random city
    private static String getRandomCity(Random random) {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Boston", "Miami", "Seattle", "Austin", "Dallas", "Atlanta", "Denver"};
        return cities[random.nextInt(cities.length)];
    }
}
