package edu.smu.smusql.bplus;

public class Search {
    
    /**
     * This method performs a standard linear search on a sorted
     * DictionaryPair[] and returns the index of the first null entry found.
     * Otherwise, this method returns a -1. This method is primarily used in
     * place of binarySearch() when the target t = null.
     * 
     * @param dps: list of dictionary pairs sorted by key within leaf node
     * @return index of the target value if found, else -1
     */
    public static int linearNullSearch(DictionaryPair[] dps) {
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] == null) {
                return i;
            }
        }
        return -1;
    }

    /**
     * This method performs a standard linear search on a list of Node[] pointers
     * and returns the index of the first null entry found. Otherwise, this
     * method returns a -1. This method is primarily used in place of
     * binarySearch() when the target t = null.
     * 
     * @param pointers: list of Node[] pointers
     * @return index of the target value if found, else -1
     */
    public static int linearNullSearch(Node[] pointers) {
        for (int i = 0; i < pointers.length; i++) {
            if (pointers[i] == null) {
                return i;
            }
        }
        return -1;
    }

}
