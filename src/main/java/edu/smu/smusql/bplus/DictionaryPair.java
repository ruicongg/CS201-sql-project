package edu.smu.smusql.bplus;

/**
 * This class represents a dictionary pair that is to be contained within the
 * leaf nodes of the B+ tree. The class implements the Comparable interface
 * so that the DictionaryPair objects can be sorted later on.
 */
class DictionaryPair implements Comparable<DictionaryPair> {
    String key;
    int indexInTable;

    DictionaryPair(String key, int indexInTable) {
        this.key = key;
        this.indexInTable = indexInTable;
    }

    // smaller keys will come first
    @Override
    public int compareTo(DictionaryPair o) {
        return key.compareTo(o.key);
    }
}