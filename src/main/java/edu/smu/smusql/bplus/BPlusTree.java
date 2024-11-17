package edu.smu.smusql.bplus;

import edu.smu.smusql.interfaces.RowEntry;
import java.util.*;
import java.io.*;

public class BPlusTree {
    // change this to experiment with different orders/fanouts (the max keys of
    // internal nodes)
    int ORDER = 20;
    int m = ORDER;
    InternalNode root;
    LeafNode firstLeaf;

    public BPlusTree() {
        this.root = null;
    }


    public void insert(String key, int index) {
        if (isEmpty()) {

            /* Flow of execution goes here only when first insert takes place */

            // Create leaf node as first node in B plus tree (root is null)
            LeafNode ln = new LeafNode(this.m, new DictionaryPair(key, index));

            // Set as first leaf node (can be used later for in-order leaf traversal)
            this.firstLeaf = ln;
            return;
        }

        // Find leaf node to insert into
        LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNodesEqualTo(this.root, key).get(0);

        // Insert into leaf node fails if node becomes overfull
        if (!ln.insert(new DictionaryPair(key, index))) {

            // Sort all the dictionary pairs with the included pair to be inserted
            ln.dictionary[ln.numPairs] = new DictionaryPair(key, index);
            ln.numPairs++;
            sortDictionary(ln.dictionary);

            // Split the sorted pairs into two halves
            int midpoint = getMidpoint();
            DictionaryPair[] halfDict = splitDictionary(ln, midpoint);

            if (ln.parent == null) {

                /* Flow of execution goes here when there is 1 node in tree */

                // Create internal node to serve as parent, use dictionary midpoint key
                String[] parent_keys = new String[this.m];
                parent_keys[0] = halfDict[0].key;
                InternalNode parent = new InternalNode(this.m, parent_keys);
                ln.parent = parent;
                parent.appendChildPointer(ln);

            } else {

                /* Flow of execution goes here when parent exists */

                // Add new key to parent for proper indexing
                String newParentKey = halfDict[0].key;
                ln.parent.keys[ln.parent.degree - 1] = newParentKey;
                Arrays.sort(ln.parent.keys, 0, ln.parent.degree);
            }

            // Create new LeafNode that holds the other half
            LeafNode newLeafNode = new LeafNode(this.m, halfDict, ln.parent);

            // Update child pointers of parent node
            int pointerIndex = ln.parent.findIndexOfPointer(ln) + 1;
            ln.parent.insertChildPointer(newLeafNode, pointerIndex);

            // Make leaf nodes siblings of one another
            newLeafNode.rightSibling = ln.rightSibling;
            if (newLeafNode.rightSibling != null) {
                newLeafNode.rightSibling.leftSibling = newLeafNode;
            }
            ln.rightSibling = newLeafNode;
            newLeafNode.leftSibling = ln;

            if (this.root == null) {

                // Set the root of B+ tree to be the parent
                this.root = ln.parent;

            } else {

                /*
                 * If parent is overfull, repeat the process up the tree,
                 * until no deficiencies are found
                 */
                InternalNode in = ln.parent;
                while (in != null) {
                    if (in.isOverfull()) {
                        splitInternalNode(in);
                    } else {
                        break;
                    }
                    in = in.parent;
                }
            }

        }
    }

    List<Integer> searchAll() {
        List<Integer> res = new ArrayList<>();
        if (isEmpty()) {
            return res;
        }
        LeafNode ln = this.firstLeaf;
        while (ln != null) {
            res.addAll(ln.getAllEntries());
            ln = ln.rightSibling;
        }
        return res;
    }

    public List<Integer> searchEqualTo(String key) {
        List<Integer> res = new ArrayList<>();
        if (isEmpty()) {
            return res;
        }
        if (this.root == null) {

            return this.firstLeaf.getRowEntriesEqualTo(key);
        }
        List<LeafNode> leafNodes = findLeafNodesEqualTo(this.root, key);
        for (LeafNode ln : leafNodes) {
            res.addAll(ln.getRowEntriesEqualTo(key));
        }
        return res;
    }

    public List<Integer> searchGreaterThan(String key) {

        List<Integer> res = new ArrayList<>();
        if (isEmpty()) {
            return res;
        }
        if (this.root == null) {
;
            return this.firstLeaf.getRowEntriesMoreThan(key);

        }
        List<LeafNode> leafNodes = findLeafNodesEqualTo(this.root, key);
        LeafNode ln = leafNodes.get(leafNodes.size() - 1);
        res.addAll(ln.getRowEntriesMoreThan(key));

        while (ln.rightSibling != null) {
            ln = ln.rightSibling;
            res.addAll(ln.getAllEntries());
        }
        return res;
    }
    
    public List<Integer> searchGreaterThanOrEqualTo(String key) {
        List<Integer> res = new ArrayList<>();
        if (isEmpty()) {
            return res;
        }
        if (this.root == null) {
            return this.firstLeaf.getRowEntriesMoreThanOrEqual(key);
        }
        List<LeafNode> leafNodes = findLeafNodesEqualTo(this.root, key);
        LeafNode ln = leafNodes.get(0);
        res.addAll(ln.getRowEntriesMoreThanOrEqual(key));
        while (ln.rightSibling != null) {
            ln = ln.rightSibling;
            res.addAll(ln.getAllEntries());
        }
        return res;
    }
    
    public List<Integer> searchLessThan(String key) {
        List<Integer> res = new ArrayList<>();
        if (isEmpty()) {
            return res;
        }
        if (this.root == null) {
            return this.firstLeaf.getRowEntriesLessThan(key);
        }
        List<LeafNode> leafNodes = findLeafNodesEqualTo(this.root, key);
        LeafNode ln = leafNodes.get(0);
        res.addAll(ln.getRowEntriesLessThan(key));
        while (ln.leftSibling != null) {
            ln = ln.leftSibling;
            res.addAll(ln.getAllEntries());
        }
        return res;
    }
    
    public List<Integer> searchLessThanOrEqualTo(String key) {
        List<Integer> res = new ArrayList<>();
        if (isEmpty()) {
            return res;
        }
        if (this.root == null) {
            return this.firstLeaf.getRowEntriesLessThanOrEqual(key);
        }
        List<LeafNode> leafNodes = findLeafNodesEqualTo(this.root, key);
        LeafNode ln = leafNodes.get(leafNodes.size() - 1);
        res.addAll(ln.getRowEntriesLessThanOrEqual(key));
        while (ln.leftSibling != null) {
            ln = ln.leftSibling;
            res.addAll(ln.getAllEntries());
        }
        return res;
    }

    /* ~~~~~~~~~~~~~~~~ HELPER FUNCTIONS ~~~~~~~~~~~~~~~~ */

    private List<LeafNode> findLeafNodesEqualTo(InternalNode node, String key) {

        List<LeafNode> leafNodes = new ArrayList<>();

        // Initialize keys and index variable
        String[] keys = node.keys;

        List<Node> childNodes = new ArrayList<>();


        int indexOfChildToAddInto= 0;
        
        while (indexOfChildToAddInto < node.degree - 1 && key.compareTo(keys[indexOfChildToAddInto]) >= 0) {
            indexOfChildToAddInto++;
        }
        childNodes.add(node.childPointers[indexOfChildToAddInto]);
        indexOfChildToAddInto++;
        while (indexOfChildToAddInto < node.degree - 1 && key.equals(keys[indexOfChildToAddInto])) {
            childNodes.add(node.childPointers[indexOfChildToAddInto]);
            indexOfChildToAddInto++;
        }
        

        /*
         * Return node if it is a LeafNode object,
         * otherwise repeat the search function a level down
         */
        if (childNodes.get(0) instanceof LeafNode) {
            for (Node childNode : childNodes) {
                leafNodes.add((LeafNode) childNode);
            }
        } else {
            for (Node childNode : childNodes) {
                leafNodes.addAll(findLeafNodesEqualTo((InternalNode) childNode, key));
            }
        }
        return leafNodes;
    }
    /**
     * This is a simple method that returns the midpoint (or lower bound
     * depending on the context of the method invocation) of the max degree m of
     * the B+ tree.
     * 
     * @return (int) midpoint/lower bound
     */
    private int getMidpoint() {
        return (int) Math.ceil((this.m + 1) / 2.0) - 1;
    }

    /**
     * This is a simple method that determines if the B+ tree is empty or not.
     * 
     * @return a boolean indicating if the B+ tree is empty or not
     */
    private boolean isEmpty() {
        return firstLeaf == null;
    }

    /**
     * This is a specialized sorting method used upon lists of DictionaryPairs
     * that may contain interspersed null values.
     * 
     * @param dictionary: a list of DictionaryPair objects
     */
    private void sortDictionary(DictionaryPair[] dictionary) {
        Arrays.sort(dictionary, new Comparator<DictionaryPair>() {
            @Override
            public int compare(DictionaryPair o1, DictionaryPair o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 == null) {
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return o1.compareTo(o2);
            }
        });
    }

    /**
     * This method modifies the InternalNode 'in' by removing all pointers within
     * the childPointers after the specified split. The method returns the removed
     * pointers in a list of their own to be used when constructing a new
     * InternalNode sibling.
     * 
     * @param in:    an InternalNode whose childPointers will be split
     * @param split: the index at which the split in the childPointers begins
     * @return a Node[] of the removed pointers
     */
    private Node[] splitChildPointers(InternalNode in, int split) {

        Node[] pointers = in.childPointers;
        Node[] halfPointers = new Node[this.m + 1];

        // Copy half of the values into halfPointers while updating original keys
        for (int i = split + 1; i < pointers.length; i++) {
            halfPointers[i - split - 1] = pointers[i];
            in.removePointer(i);
        }

        return halfPointers;
    }

    /**
     * This method splits a single dictionary into two dictionaries where all
     * dictionaries are of equal length, but each of the resulting dictionaries
     * holds half of the original dictionary's non-null values. This method is
     * primarily used when splitting a node within the B+ tree. The dictionary of
     * the specified LeafNode is modified in place. The method returns the
     * remainder of the DictionaryPairs that are no longer within ln's dictionary.
     * 
     * @param ln:    list of DictionaryPairs to be split
     * @param split: the index at which the split occurs
     * @return DictionaryPair[] of the two split dictionaries
     */
    private DictionaryPair[] splitDictionary(LeafNode ln, int split) {

        DictionaryPair[] dictionary = ln.dictionary;

        /*
         * Initialize two dictionaries that each hold half of the original
         * dictionary values
         */
        DictionaryPair[] halfDict = new DictionaryPair[this.m];

        // Copy half of the values into halfDict
        for (int i = split; i < dictionary.length; i++) {
            halfDict[i - split] = dictionary[i];
            ln.delete(i);
        }

        return halfDict;
    }

    /**
     * When an insertion into the B+ tree causes an overfull node, this method
     * is called to remedy the issue, i.e. to split the overfull node. This method
     * calls the sub-methods of splitKeys() and splitChildPointers() in order to
     * split the overfull node.
     * 
     * @param in: an overfull InternalNode that is to be split
     */
    private void splitInternalNode(InternalNode in) {

        // Acquire parent
        InternalNode parent = in.parent;

        // Split keys and pointers in half
        int midpoint = getMidpoint();
        String newParentKey = in.keys[midpoint];
        String[] halfKeys = splitKeys(in.keys, midpoint);
        Node[] halfPointers = splitChildPointers(in, midpoint);

        // Change degree of original InternalNode in
        in.degree = Search.linearNullSearch(in.childPointers);

        // Create new sibling internal node and add half of keys and pointers
        InternalNode sibling = new InternalNode(this.m, halfKeys, halfPointers);
        for (Node pointer : halfPointers) {
            if (pointer != null) {
                pointer.parent = sibling;
            }
        }

        // Make internal nodes siblings of one another
        sibling.rightSibling = in.rightSibling;
        if (sibling.rightSibling != null) {
            sibling.rightSibling.leftSibling = sibling;
        }
        in.rightSibling = sibling;
        sibling.leftSibling = in;

        if (parent == null) {

            // Create new root node and add midpoint key and pointers
            String[] keys = new String[this.m];
            keys[0] = newParentKey;
            InternalNode newRoot = new InternalNode(this.m, keys);
            newRoot.appendChildPointer(in);
            newRoot.appendChildPointer(sibling);
            this.root = newRoot;

            // Add pointers from children to parent
            in.parent = newRoot;
            sibling.parent = newRoot;

        } else {

            // Add key to parent
            parent.keys[parent.degree - 1] = newParentKey;
            Arrays.sort(parent.keys, 0, parent.degree);

            // Set up pointer to new sibling
            int pointerIndex = parent.findIndexOfPointer(in) + 1;
            parent.insertChildPointer(sibling, pointerIndex);
            sibling.parent = parent;
        }
    }

    /**
     * This method modifies a list of String-typed objects that represent keys
     * by removing half of the keys and returning them in a separate String[].
     * This method is used when splitting an InternalNode object.
     * 
     * @param keys:  a list of String objects
     * @param split: the index where the split is to occur
     * @return String[] of removed keys
     */
    private String[] splitKeys(String[] keys, int split) {

        String[] halfKeys = new String[this.m];

        // Remove split-indexed value from keys
        keys[split] = null;

        // Copy half of the values into halfKeys while updating original keys
        for (int i = split + 1; i < keys.length; i++) {
            halfKeys[i - split - 1] = keys[i];
            keys[i] = null;
        }

        return halfKeys;
    }
}