package edu.smu.smusql.bst;

import edu.smu.smusql.interfaces.RowEntry;

public class BSTNode {
    RowEntry row;
    BSTNode left, right;

    public BSTNode(RowEntry row) {
        this.row = row;
        this.left = this.right = null;
    }
}