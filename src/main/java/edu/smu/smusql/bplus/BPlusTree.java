package edu.smu.smusql.bplus;

import edu.smu.smusql.interfaces.RowEntry;
import java.util.*;

public class BPlusTree {
    private static final int ORDER = 4; // Order of the B+ tree
    private Node root;
    private Node firstLeaf;

    private abstract class Node {
        List<String> keys;
        int keyCount;
        Node parent;
        boolean isLeaf;

        Node() {
            this.keys = new ArrayList<>();
            this.keyCount = 0;
            this.parent = null;
            this.isLeaf = false;
        }
    }

    private class InternalNode extends Node {
        List<Node> children;

        InternalNode() {
            super();
            this.children = new ArrayList<>();
        }
    }

    private class LeafNode extends Node {
        List<List<RowEntry>> values;
        LeafNode nextLeaf;

        LeafNode() {
            super();
            this.isLeaf = true;
            this.values = new ArrayList<>();
            this.nextLeaf = null;
        }
    }

    public BPlusTree() {
        root = new LeafNode();
        firstLeaf = (LeafNode) root;
    }

    public void insert(String key, RowEntry value) {
        if (root == null) {
            root = new LeafNode();
            firstLeaf = (LeafNode) root;
        }

        LeafNode leaf = findLeafNode(key);
        int insertionPoint = 0;
        while (insertionPoint < leaf.keyCount && leaf.keys.get(insertionPoint).compareTo(key) < 0) {
            insertionPoint++;
        }

        if (insertionPoint < leaf.keyCount && leaf.keys.get(insertionPoint).equals(key)) {
            // Key exists, add to existing list
            leaf.values.get(insertionPoint).add(value);
            return;
        }

        // Insert new key-value pair
        leaf.keys.add(insertionPoint, key);
        leaf.values.add(insertionPoint, new ArrayList<>(Collections.singletonList(value)));
        leaf.keyCount++;

        if (leaf.keyCount >= ORDER) {
            splitLeafNode(leaf);
        }
    }

    private void splitLeafNode(LeafNode leaf) {
        LeafNode newLeaf = new LeafNode();
        int splitPoint = (ORDER + 1) / 2;
        
        // Move half of the entries to the new leaf
        newLeaf.keys = new ArrayList<>(leaf.keys.subList(splitPoint, leaf.keyCount));
        newLeaf.values = new ArrayList<>(leaf.values.subList(splitPoint, leaf.keyCount));
        newLeaf.keyCount = newLeaf.keys.size();
        
        // Update original leaf
        leaf.keys = new ArrayList<>(leaf.keys.subList(0, splitPoint));
        leaf.values = new ArrayList<>(leaf.values.subList(0, splitPoint));
        leaf.keyCount = leaf.keys.size();

        // Update leaf pointers
        newLeaf.nextLeaf = leaf.nextLeaf;
        leaf.nextLeaf = newLeaf;
        
        String newKey = newLeaf.keys.get(0);
        
        if (leaf == root) {
            InternalNode newRoot = new InternalNode();
            newRoot.keys.add(newKey);
            newRoot.children.add(leaf);
            newRoot.children.add(newLeaf);
            newRoot.keyCount = 1;
            
            root = newRoot;
            leaf.parent = newRoot;
            newLeaf.parent = newRoot;
        } else {
            insertInParent(leaf, newKey, newLeaf);
        }
    }

    private void insertInParent(Node left, String key, Node right) {
        InternalNode parent = (InternalNode) left.parent;
        
        int insertionPoint = 0;
        while (insertionPoint < parent.keyCount && parent.keys.get(insertionPoint).compareTo(key) < 0) {
            insertionPoint++;
        }
        
        parent.keys.add(insertionPoint, key);
        parent.children.add(insertionPoint + 1, right);
        parent.keyCount++;
        right.parent = parent;
        
        if (parent.keyCount >= ORDER) {
            splitInternalNode(parent);
        }
    }

    private void splitInternalNode(InternalNode node) {
        InternalNode newNode = new InternalNode();
        int splitPoint = ORDER / 2;
        
        String promotedKey = node.keys.get(splitPoint);
        
        // Move half of the entries to the new node
        newNode.keys = new ArrayList<>(node.keys.subList(splitPoint + 1, node.keyCount));
        newNode.children = new ArrayList<>(node.children.subList(splitPoint + 1, node.children.size()));
        newNode.keyCount = newNode.keys.size();
        
        // Update parent pointers for moved children
        for (Node child : newNode.children) {
            child.parent = newNode;
        }
        
        // Update original node
        node.keys = new ArrayList<>(node.keys.subList(0, splitPoint));
        node.children = new ArrayList<>(node.children.subList(0, splitPoint + 1));
        node.keyCount = node.keys.size();
        
        if (node == root) {
            InternalNode newRoot = new InternalNode();
            newRoot.keys.add(promotedKey);
            newRoot.children.add(node);
            newRoot.children.add(newNode);
            newRoot.keyCount = 1;
            
            root = newRoot;
            node.parent = newRoot;
            newNode.parent = newRoot;
        } else {
            insertInParent(node, promotedKey, newNode);
        }
    }

    private LeafNode findLeafNode(String key) {
        Node current = root;
        while (!current.isLeaf) {
            InternalNode internalNode = (InternalNode) current;
            int i = 0;
            while (i < internalNode.keyCount && key.compareTo(internalNode.keys.get(i)) >= 0) {
                i++;
            }
            current = internalNode.children.get(i);
        }
        return (LeafNode) current;
    }

    public List<RowEntry> search(String key) {
        LeafNode leaf = findLeafNode(key);
        for (int i = 0; i < leaf.keyCount; i++) {
            if (leaf.keys.get(i).equals(key)) {
                return leaf.values.get(i);
            }
        }
        return new ArrayList<>();
    }

    public List<RowEntry> searchRange(String startKey, String endKey) {
        List<RowEntry> result = new ArrayList<>();
        LeafNode current = findLeafNode(startKey);
        
        boolean started = false;
        while (current != null) {
            for (int i = 0; i < current.keyCount; i++) {
                String currentKey = current.keys.get(i);
                if (currentKey.compareTo(startKey) >= 0 && currentKey.compareTo(endKey) <= 0) {
                    started = true;
                    result.addAll(current.values.get(i));
                } else if (started) {
                    return result;
                }
            }
            current = current.nextLeaf;
        }
        return result;
    }

    public void delete(String key, RowEntry value) {
        LeafNode leaf = findLeafNode(key);
        for (int i = 0; i < leaf.keyCount; i++) {
            if (leaf.keys.get(i).equals(key)) {
                leaf.values.get(i).remove(value);
                if (leaf.values.get(i).isEmpty()) {
                    leaf.keys.remove(i);
                    leaf.values.remove(i);
                    leaf.keyCount--;
                }
                return;
            }
        }
    }

    public List<RowEntry> getAllValues() {
        List<RowEntry> result = new ArrayList<>();
        LeafNode current = (LeafNode) firstLeaf;
        while (current != null) {
            for (List<RowEntry> valueList : current.values) {
                result.addAll(valueList);
            }
            current = current.nextLeaf;
        }
        return result;
    }
} 