// File: BSTTreeTester.java

package edu.smu.smusql.bst;

import edu.smu.smusql.interfaces.RowEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class BSTTreeTester {
    private static final int LIMIT = 10;

    public static void testBST() {
        BinarySearchTree bst = new BinarySearchTree();
        List<Integer> idList = generateUniqueRandomIds(LIMIT);

        System.out.println("=== Starting BST Test ===\n");

        // Insert nodes into BST with random IDs
        System.out.println("Inserting nodes into BST with random IDs:");
        for (int i = 0; i < LIMIT; i++) {
            String id = String.valueOf(idList.get(i));
            String name = "Employee" + (i + 1);
            int age = 25 + (i % 30); // Ages between 25 and 54
            String department = getDepartment(i);
            String salary = String.valueOf(50000 + (i * 5000));

            RowEntry row = new RowEntry();
            row.addOrUpdateValue("id", id);
            row.addOrUpdateValue("name", name);
            row.addOrUpdateValue("age", String.valueOf(age));
            row.addOrUpdateValue("department", department);
            row.addOrUpdateValue("salary", salary);

            System.out.println("Inserting Row: " + row);
            bst.insert(row, "id");
            System.out.println("BST after insertion:");
            bst.printTree();
            System.out.println();
        }

        // Search for a specific node
        String searchKey = String.valueOf(idList.get(LIMIT / 2)); // Search for a middle ID
        System.out.println("Searching for Node with ID " + searchKey + ":");
        RowEntry found = bst.search(searchKey, "id");
        if (found != null) {
            System.out.println("Search Result: " + found + "\n");
        } else {
            System.out.println("ID " + searchKey + " not found in BST.\n");
        }

        // Delete a node
        String deleteKey = String.valueOf(idList.get(2)); // Delete the third inserted ID
        System.out.println("Deleting Node with ID " + deleteKey + ":");
        bst.delete(deleteKey, "id");
        System.out.println("BST after deletion:");
        bst.printTree();
        System.out.println();

        // Display all nodes after deletion
        System.out.println("BST In-Order Traversal After Deletion:");
        List<RowEntry> allRows = bst.getAllRows();
        for (RowEntry row : allRows) {
            System.out.println(row);
        }
        System.out.println("=== BST Test Completed ===");
    }

    /**
     * Generates a list of unique random integers.
     *
     * @param limit The number of unique random IDs to generate.
     * @return A shuffled list of unique IDs.
     */
    private static List<Integer> generateUniqueRandomIds(int limit) {
        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= limit * 10; i++) { // Generate a larger pool to ensure uniqueness
            ids.add(i);
        }
        Collections.shuffle(ids, new Random());
        List<Integer> uniqueIds = new ArrayList<>(ids.subList(0, limit));
        System.out.println("Generated Unique Random IDs: " + uniqueIds + "\n");
        return uniqueIds;
    }

    // Helper method to assign departments
    private static String getDepartment(int index) {
        String[] departments = { "HR", "Engineering", "Finance", "Marketing", "Sales" };
        return departments[index % departments.length];
    }

    // Main method to run the tester independently
    public static void main(String[] args) {
        testBST();
    }
}