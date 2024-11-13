package edu.smu.smusql.bst;

import java.util.Map;

public class BSTNode {
    Map<String, String> row;  // Stores the row data as a map
    BSTNode left, right;

    public BSTNode(Map<String, String> row) {
        this.row = row;
        this.left = this.right = null;
    }
}
