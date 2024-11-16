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
        try {
            int thisKey = Integer.parseInt(key);
            int otherKey = Integer.parseInt(o.key);
            return Integer.compare(thisKey, otherKey);
        } catch (NumberFormatException e) {
            // If not numbers, fall back to string comparison
            return key.compareTo(o.key);
        }
    }
}