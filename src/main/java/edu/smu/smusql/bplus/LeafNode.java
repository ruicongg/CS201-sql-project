package edu.smu.smusql.bplus;
import java.util.*;
import edu.smu.smusql.interfaces.RowEntry;
/**
 * This class represents the leaf nodes within the B+ tree that hold
 * dictionary pairs. The leaf node has no children. The leaf node has a
 * minimum and maximum number of dictionary pairs it can hold, as specified
 * by m, the max degree of the B+ tree. The leaf nodes form a doubly linked
 * list that, i.e. each leaf node has a left and right sibling
 */
class LeafNode extends Node {
    int maxNumPairs;
    int minNumPairs;
    int numPairs;
    LeafNode leftSibling;
    LeafNode rightSibling;
    DictionaryPair[] dictionary;

    List<RowEntry> getRowEntriesEqualTo(String key) {
        List<RowEntry> rowEntries = new ArrayList<>();
        DictionaryPair[] copy = new DictionaryPair[numPairs];
        for (int i = 0; i < numPairs; i++) {
            copy[i] = dictionary[i];
        }
        int foundIndex = Arrays.binarySearch(copy, new DictionaryPair(key, null));
        if (foundIndex < 0) {
            return rowEntries;
        }
        for (int i = foundIndex - 1; i >= 0 && dictionary[i].key.equals(key); i--) {
            RowEntry rowEntry = dictionary[i].rowEntry;
            if (!rowEntry.isDeleted()) {
                rowEntries.add(rowEntry);
            }
        }
        for (int i = foundIndex + 1; i < numPairs && dictionary[i].key.equals(key); i++) {
            RowEntry rowEntry = dictionary[i].rowEntry;
            if (!rowEntry.isDeleted()) {
                rowEntries.add(rowEntry);
            }
        }
        return rowEntries;
    }

    List<RowEntry> getRowEntriesMoreThan(String key) {
        List<RowEntry> rowEntries = new ArrayList<>();
        DictionaryPair[] copy = new DictionaryPair[numPairs];
        for (int i = 0; i < numPairs; i++) {
            copy[i] = dictionary[i];
        }
        int foundIndex = Arrays.binarySearch(copy, new DictionaryPair(key, null));
        int traversalIndex; 
        if (foundIndex >= 0) {
            traversalIndex = foundIndex + 1;
            while (traversalIndex < numPairs && dictionary[traversalIndex].key.equals(key)) {
                traversalIndex++;
            }
        } else {
            traversalIndex = -(foundIndex + 1); // Convert to insertion point
        }
        for (int i = traversalIndex; i < numPairs; i++) {
            RowEntry rowEntry = dictionary[i].rowEntry;
            if (!rowEntry.isDeleted()) {
                rowEntries.add(rowEntry);
            }
        }
        return rowEntries;
    }

    List<RowEntry> getRowEntriesMoreThanOrEqual(String key) {
        List<RowEntry> rowEntries = new ArrayList<>();
        DictionaryPair[] copy = new DictionaryPair[numPairs];
        for (int i = 0; i < numPairs; i++) {
            copy[i] = dictionary[i];
        }
        int foundIndex = Arrays.binarySearch(copy, new DictionaryPair(key, null));
        int traversalIndex;
        if (foundIndex >= 0) {
            traversalIndex = foundIndex;
            addEntriesBefore(traversalIndex, rowEntries, key);
        } else {
            traversalIndex = -(foundIndex + 1); // Convert to insertion point
        }
        for (int i = traversalIndex; i < numPairs; i++) {
            RowEntry rowEntry = dictionary[i].rowEntry;
            if (!rowEntry.isDeleted()) {
                rowEntries.add(rowEntry);
            }
        }
        return rowEntries;
    }

    private void addEntriesBefore(int index, List<RowEntry> rowEntries, String key) {
        int i = index - 1;
        while (i >= 0 && dictionary[i].key.equals(key)) {
            RowEntry rowEntry = dictionary[i].rowEntry;
            if (!rowEntry.isDeleted()) {
                rowEntries.add(rowEntry);
            }
            i--;
        }
    }

    List<RowEntry> getRowEntriesLessThan(String key) {
        List<RowEntry> rowEntries = new ArrayList<>();
        DictionaryPair[] copy = new DictionaryPair[numPairs];
        for (int i = 0; i < numPairs; i++) {
            copy[i] = dictionary[i];
        }
        int foundIndex = Arrays.binarySearch(copy, new DictionaryPair(key, null));
        int traversalIndex;
        if (foundIndex >= 0) {
            traversalIndex = foundIndex - 1;
            while (traversalIndex > 0 && dictionary[traversalIndex].key.equals(key)) {
                traversalIndex--;
            }
        } else {
            traversalIndex = -(foundIndex + 1); // Convert to insertion point
        }
        for (int i = 0; i < traversalIndex; i++) {
            RowEntry rowEntry = dictionary[i].rowEntry;
            if (!rowEntry.isDeleted()) {
                rowEntries.add(rowEntry);
            }
        }
        return rowEntries;
    }

    List<RowEntry> getRowEntriesLessThanOrEqual(String key) {
        List<RowEntry> rowEntries = new ArrayList<>();
        DictionaryPair[] copy = new DictionaryPair[numPairs];
        for (int i = 0; i < numPairs; i++) {
            copy[i] = dictionary[i];
        }
        int foundIndex = Arrays.binarySearch(copy, new DictionaryPair(key, null));
        int traversalIndex;
        if (foundIndex >= 0) {
            traversalIndex = foundIndex;
            addEntriesAfter(traversalIndex, rowEntries, key);
        } else {
            traversalIndex = -(foundIndex + 1); // Convert to insertion point
        }
        for (int i = 0; i < traversalIndex; i++) {
            RowEntry rowEntry = dictionary[i].rowEntry;
            if (!rowEntry.isDeleted()) {
                rowEntries.add(rowEntry);
            }
        }
        return rowEntries;
    }

    private void addEntriesAfter(int index, List<RowEntry> rowEntries, String key) {
        int i = index + 1;
        while (i < numPairs && dictionary[i].key.equals(key)) {
            RowEntry rowEntry = dictionary[i].rowEntry;
            if (!rowEntry.isDeleted()) {
                rowEntries.add(rowEntry);
            }
            i++;
        }
    }

    List<RowEntry> getAllEntries() {
        List<RowEntry> rowEntries = new ArrayList<>();
        for (int i = 0; i < numPairs; i++) {
            RowEntry rowEntry = dictionary[i].rowEntry;
            if (!rowEntry.isDeleted()) {
                rowEntries.add(rowEntry);
            }
        }
        return rowEntries;
    }
    /**
     * Given an index, this method sets the dictionary pair at that index
     * within the dictionary to null.
     * 
     * @param index: the location within the dictionary to be set to null
     */
    void delete(int index) {

        // Delete dictionary pair from leaf
        this.dictionary[index] = null;

        // Decrement numPairs
        numPairs--;
    }

    /**
     * This method attempts to insert a dictionary pair within the dictionary
     * of the LeafNode object. If it succeeds, numPairs increments, the
     * dictionary is sorted, and the boolean true is returned. If the method
     * fails, the boolean false is returned.
     * 
     * @param dp: the dictionary pair to be inserted
     * @return a boolean indicating whether or not the insert was successful
     */
    boolean insert(DictionaryPair dp) {
        if (this.isFull()) {

            /* Flow of execution goes here when numPairs == maxNumPairs */

            return false;
        } else {

            // Insert dictionary pair, increment numPairs, sort dictionary
            this.dictionary[numPairs] = dp;
            numPairs++;
            Arrays.sort(this.dictionary, 0, numPairs);

            return true;
        }
    }

    /**
     * This simple method determines if the LeafNode is full, i.e. the
     * numPairs within the LeafNode is equal to the maximum number of pairs.
     * 
     * @return a boolean indicating whether or not the LeafNode is full
     */
    boolean isFull() {
        return numPairs == maxNumPairs;
    }

    /**
     * Constructor for root node
     * 
     * @param m:  order of B+ tree that is used to calculate maxNumPairs and
     *            minNumPairs
     * @param dp: first dictionary pair insert into new node
     */
    LeafNode(int m, DictionaryPair dp) {
        this.maxNumPairs = m - 1;
        this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
        this.dictionary = new DictionaryPair[m];
        this.numPairs = 0;
        this.insert(dp);
    }

    /**
     * Constructor
     * 
     * @param dps:    list of DictionaryPair objects to be immediately inserted
     *                into new LeafNode object
     * @param m:      order of B+ tree that is used to calculate maxNumPairs and
     *                minNumPairs
     * @param parent: parent of newly created child LeafNode
     */
    LeafNode(int m, DictionaryPair[] dps, InternalNode parent) {
        this.maxNumPairs = m - 1;
        this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
        this.dictionary = dps;
        this.numPairs = Search.linearNullSearch(dps);
        this.parent = parent;
    }
}