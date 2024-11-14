package edu.smu.smusql.bst;

import edu.smu.smusql.interfaces.RowEntry;

import java.util.Map;

public class BSTNode extends RowEntry {
    BSTNode left, right;

    public BSTNode(Map<String, String> row) {
        super(row);
        this.left = this.right = null;
    }
}