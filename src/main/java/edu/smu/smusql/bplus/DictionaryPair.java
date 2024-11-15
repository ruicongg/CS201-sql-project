package edu.smu.smusql.bplus;
import edu.smu.smusql.interfaces.RowEntry;

/**
 * This class represents a dictionary pair that is to be contained within the
 * leaf nodes of the B+ tree. The class implements the Comparable interface
 * so that the DictionaryPair objects can be sorted later on.
 */
class DictionaryPair implements Comparable<DictionaryPair> {
    String key;
    RowEntry rowEntry;

    DictionaryPair(String key, RowEntry rowEntry) {
        this.key = key;
        this.rowEntry = rowEntry;
    }

    // smaller keys will come first
    @Override
    public int compareTo(DictionaryPair o) {
        return key.compareTo(o.key);
    }
}