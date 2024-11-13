package edu.smu.smusql.bst;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class BinarySearchTreeTest {
    public static void main(String[] args) {
        BinarySearchTree bst = new BinarySearchTree();

        // Edge Case 1: Inserting nodes with duplicate IDs
        System.out.println("\n--- Testing Duplicate Insertions ---");
        Map<String, String> row1 = Map.of("id", "20", "name", "Alice", "age", "23");
        Map<String, String> row2 = Map.of("id", "20", "name", "Bob", "age", "25");
        bst.insert(row1, "id");
        bst.insert(row2, "id"); // Should not insert a duplicate key

        // bst.printTree();
        printAllRows(bst);

        // Edge Case 2: Deleting the root repeatedly
        System.out.println("\n--- Deleting the Root Node Repeatedly ---");
        Map<String, String> row3 = Map.of("id", "10", "name", "Charlie", "age", "30");
        Map<String, String> row4 = Map.of("id", "5", "name", "David", "age", "35");
        Map<String, String> row5 = Map.of("id", "25", "name", "Eve", "age", "40");
        bst.insert(row3, "id");
        bst.insert(row4, "id");
        bst.insert(row5, "id");

        System.out.println("\nInitial Tree:");
        bst.printTree();

        System.out.println("\nDeleting root node (id=20)");
        bst.delete("20", "id");
        bst.printTree();

        System.out.println("\nDeleting root node (id=10)");
        bst.delete("10", "id");
        bst.printTree();

        System.out.println("\nDeleting root node (id=25)");
        bst.delete("25", "id");
        bst.printTree();

        // Edge Case 3: Deleting non-existent nodes
        System.out.println("\n--- Deleting Non-existent Nodes ---");
        bst.delete("100", "id"); // Node with id=100 does not exist
        bst.printTree();

        // Edge Case 4: Searching for non-existent keys
        System.out.println("\n--- Searching for Non-existent Keys ---");
        Map<String, String> result = bst.search("100", "id");
        System.out.println("Search result for id=100: " + (result != null ? result : "Not found"));

        bst.delete("5", "id");
        bst.printTree();

        // Edge Case 5: Inserting a single node and deleting it
        System.out.println("\n--- Inserting and Deleting a Single Node ---");
        Map<String, String> singleNode = Map.of("id", "999", "name", "Zara", "age", "28");
        bst.insert(singleNode, "id");
       

        System.out.println("\nDeleting the only node (id=999)");
        bst.delete("999", "id");
        bst.printTree();

        // Edge Case 6: Handling large values
        System.out.println("\n--- Inserting Large IDs ---");
        Map<String, String> largeValue1 = Map.of("id", "100000", "name", "Large1", "age", "50");
        Map<String, String> largeValue2 = Map.of("id", "500000", "name", "Large2", "age", "55");
        bst.insert(largeValue1, "id");
        bst.insert(largeValue2, "id");
        

        System.out.println("\n--- Final Searches ---");
        result = bst.search("100000", "id");
        System.out.println("Search result for id=100000: " + (result != null ? result : "Not found"));
        result = bst.search("500000", "id");
        System.out.println("Search result for id=500000: " + (result != null ? result : "Not found"));
    }

    // Helper method to print all rows in sorted order
    private static void printAllRows(BinarySearchTree bst) {
        List<Map<String, String>> allRows = bst.getAllRows();
        for (Map<String, String> row : allRows) {
            System.out.println(row);
        }
    }
}
