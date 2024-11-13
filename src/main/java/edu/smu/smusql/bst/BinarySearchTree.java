package edu.smu.smusql.bst;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BinarySearchTree {
    private BSTNode root;

    public BinarySearchTree() {
        this.root = null;
    }
 
    // Insertion method
    public void insert(Map<String, String> row, String primaryKey) {
        root = insertRec(root, row, primaryKey);
        System.out.println("Inserted row with primary key " + row.get(primaryKey) + "\n");
        printTree();
    }

    private BSTNode insertRec(BSTNode node, Map<String, String> row, String primaryKey) {
        if (node == null) {
            System.out.println("Inserting new node with primary key " + row.get(primaryKey));
            return new BSTNode(row);
        }
    
        int key = Integer.parseInt(row.get(primaryKey));
        int nodeKey = Integer.parseInt(node.row.get(primaryKey));
    
        // Check for duplicate key
        if (key == nodeKey) {
            System.out.println("Duplicate key detected: " + key + ". Insertion aborted.");
            return node; // Do not insert duplicates, return the existing node
        }
    
        if (key < nodeKey) {
            System.out.println("Going left: inserting " + key + " < " + nodeKey);
            node.left = insertRec(node.left, row, primaryKey);
        } else if (key > nodeKey) {
            System.out.println("Going right: inserting " + key + " > " + nodeKey);
            node.right = insertRec(node.right, row, primaryKey);
        }
        return node;
    }
    

    // Search method
    public Map<String, String> search(String key, String primaryKey) {
        return searchRec(root, key, primaryKey);
    }

    private Map<String, String> searchRec(BSTNode node, String key, String primaryKey) {
        if (node == null) {
            System.out.println("Key " + key + " not found in the tree.\n");
            return null;  // Key not found
        }
    
        int intKey = Integer.parseInt(key);
        int nodeKey = Integer.parseInt(node.row.get(primaryKey));
    
        System.out.println("Comparing key " + key + " with node key " + nodeKey);
    
        if (intKey == nodeKey) {
            System.out.println("Key " + key + " found with data: " + node.row + "\n");
            return node.row;  // Found the row
        } else if (intKey < nodeKey) {
            System.out.println("Going left: " + key + " < " + nodeKey);
            return searchRec(node.left, key, primaryKey);  // Search left subtree
        } else {
            System.out.println("Going right: " + key + " > " + nodeKey);
            return searchRec(node.right, key, primaryKey);  // Search right subtree
        }
    }
    
    // Method to retrieve all rows in sorted order
    public List<Map<String, String>> getAllRows() {
        List<Map<String, String>> rows = new ArrayList<>();
        inOrderTraversal(root, rows);
        return rows;
    }

    // Helper method for in-order traversal
    private void inOrderTraversal(BSTNode node, List<Map<String, String>> rows) {
        if (node != null) {
            inOrderTraversal(node.left, rows);  // Visit left subtree
            System.out.println("Visiting node with primary key: " + node.row.get("id"));
            rows.add(node.row);  // Visit node itself
            inOrderTraversal(node.right, rows); // Visit right subtree
        }
    }

// Delete method
public void delete(String key, String primaryKey) {
    root = deleteRec(root, key, primaryKey);
    System.out.println("Deleted node with primary key " + key + "\n");
}

private BSTNode deleteRec(BSTNode node, String key, String primaryKey) {
    if (node == null) {
        System.out.println("Key " + key + " not found for deletion.");
        return null; // Key not found
    }

    int intKey = Integer.parseInt(key);
    int nodeKey = Integer.parseInt(node.row.get(primaryKey));

    // Locate the node to delete
    if (intKey < nodeKey) {
        System.out.println("Going left to delete " + key + " < " + nodeKey);
        node.left = deleteRec(node.left, key, primaryKey);
    } else if (intKey > nodeKey) {
        System.out.println("Going right to delete " + key + " > " + nodeKey);
        node.right = deleteRec(node.right, key, primaryKey);
    } else {
        System.out.println("Node with key " + key + " found for deletion.");

        // Case 1: Node has no children
        if (node.left == null && node.right == null) {
            return null;
        }
        // Case 2: Node has only one child
        else if (node.left == null) {
            return node.right;
        } else if (node.right == null) {
            return node.left;
        }
        // Case 3: Node has two children
        else {
            // Find the in-order successor (smallest in the right subtree)
            BSTNode successor = findMin(node.right);
            node.row = successor.row; // Replace node's data with successor's data
            node.right = deleteRec(node.right, successor.row.get(primaryKey), primaryKey); // Delete successor
        }
    }
    return node;
}

// Helper method to find the minimum node in a subtree
private BSTNode findMin(BSTNode node) {
    while (node.left != null) {
        node = node.left;
    }
    return node;
}

// Method to print the tree in ASCII format
public void printTree() {
    if (root == null) {
        System.out.println("Tree is empty.");
    } else {
        printTreeRec(root, "", true);
    }
}

private void printTreeRec(BSTNode node, String prefix, boolean isRight) {
    if (node != null) {
        // Print the current node with its prefix
        System.out.println(prefix + (isRight ? "└── " : "├── ") + "id=" + node.row.get("id"));

        // Recursively print the left and right subtrees with adjusted prefixes
        printTreeRec(node.right, prefix + (isRight ? "    " : "│   "), false);
        printTreeRec(node.left, prefix + (isRight ? "    " : "│   "), true);
    }
}

    // Additional methods for search and delete can be added here in subsequent steps
}
