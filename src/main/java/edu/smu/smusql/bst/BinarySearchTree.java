package edu.smu.smusql.bst;

// Import RowEntry
import edu.smu.smusql.interfaces.RowEntry;
import java.util.ArrayList;
import java.util.List;

public class BinarySearchTree {
    private BSTNode root;

    public BinarySearchTree() {
        this.root = null;
    }

    // Insertion method
    // Changed parameter type to RowEntry
    public void insert(RowEntry row, String primaryKey) {
        root = insertRec(root, row, primaryKey);
        // System.out.println("Inserted row with primary key " +
        // row.getValue(primaryKey) + "\n");
        // printTree();
    }

    // Helper method for insertion
    private BSTNode insertRec(BSTNode node, RowEntry row, String primaryKey) {
        if (node == null) {
            // System.out.println("Inserting new node with primary key " +
            // row.getValue(primaryKey));
            return new BSTNode(row);
        }

        // Access primary key values using row.getValue()
        int key = Integer.parseInt(row.getValue(primaryKey));
        int nodeKey = Integer.parseInt(node.row.getValue(primaryKey));

        // Check for duplicate key
        if (key == nodeKey) {
            // System.out.println("Duplicate key detected: " + key + ". Insertion
            // aborted.");
            return node; // Do not insert duplicates
        }

        if (key < nodeKey) {
            // System.out.println("Going left: inserting " + key + " < " + nodeKey);
            node.left = insertRec(node.left, row, primaryKey);
        } else {
            // System.out.println("Going right: inserting " + key + " > " + nodeKey);
            node.right = insertRec(node.right, row, primaryKey);
        }
        return node;
    }

    // Search method
    // Changed return type to RowEntry
    public RowEntry search(String key, String primaryKey) {
        return searchRec(root, key, primaryKey);
    }

    private RowEntry searchRec(BSTNode node, String key, String primaryKey) {
        if (node == null) {
            // System.out.println("Key " + key + " not found in the tree.\n");
            return null; // Key not found
        }

        int intKey = Integer.parseInt(key);
        int nodeKey = Integer.parseInt(node.row.getValue(primaryKey));

        // System.out.println("Comparing key " + key + " with node key " + nodeKey);

        if (intKey == nodeKey) {
            // System.out.println("Key " + key + " found with data: " + node.row + "\n");
            return node.row; // Found the row
        } else if (intKey < nodeKey) {
            // System.out.println("Going left: " + key + " < " + nodeKey);
            return searchRec(node.left, key, primaryKey); // Search left subtree
        } else {
            // System.out.println("Going right: " + key + " > " + nodeKey);
            return searchRec(node.right, key, primaryKey); // Search right subtree
        }
    }

    // Method to retrieve all rows in sorted order
    // Updated to return List<RowEntry>
    public List<RowEntry> getAllRows() {
        List<RowEntry> rows = new ArrayList<>();
        inOrderTraversal(root, rows);
        return rows;
    }

    // Helper method for in-order traversal
    private void inOrderTraversal(BSTNode node, List<RowEntry> rows) {
        if (node != null) {
            inOrderTraversal(node.left, rows); // Visit left subtree
            // System.out.println("Visiting node with primary key: " +
            // node.row.getValue("id"));
            rows.add(node.row); // Visit node itself
            inOrderTraversal(node.right, rows); // Visit right subtree
        }
    }

    // Delete method
    public void delete(String key, String primaryKey) {
        root = deleteRec(root, key, primaryKey);
        // System.out.println("Deleted node with primary key " + key + "\n");
        // printTree();
    }

    private BSTNode deleteRec(BSTNode node, String key, String primaryKey) {
        if (node == null) {
            // System.out.println("Key " + key + " not found for deletion.");
            return null; // Key not found
        }

        int intKey = Integer.parseInt(key);
        int nodeKey = Integer.parseInt(node.row.getValue(primaryKey));

        // Locate the node to delete
        if (intKey < nodeKey) {
            // System.out.println("Going left to delete " + key + " < " + nodeKey);
            node.left = deleteRec(node.left, key, primaryKey);
        } else if (intKey > nodeKey) {
            // System.out.println("Going right to delete " + key + " > " + nodeKey);
            node.right = deleteRec(node.right, key, primaryKey);
        } else {
            // System.out.println("Node with key " + key + " found for deletion.");

            // Case 1: No child
            if (node.left == null && node.right == null) {
                return null;
            }
            // Case 2: One child
            else if (node.left == null) {
                return node.right;
            } else if (node.right == null) {
                return node.left;
            }
            // Case 3: Two children
            else {
                // Find the in-order successor (smallest in the right subtree)
                BSTNode successor = findMin(node.right);
                node.row = successor.row; // Replace node's data with successor's data
                node.right = deleteRec(node.right, successor.row.getValue(primaryKey), primaryKey); // Delete successor
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
            System.out.println(prefix + (isRight ? "└── " : "├── ") + "id=" + node.row.getValue("id"));

            // Recursively print the left and right subtrees with adjusted prefixes
            printTreeRec(node.right, prefix + (isRight ? "    " : "│   "), false);
            printTreeRec(node.left, prefix + (isRight ? "    " : "│   "), true);
        }
    }
}