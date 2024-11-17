package edu.smu.smusql.bplus;

/**
 * This class represents the internal nodes within the B+ tree that traffic
 * all search/insert/delete operations. An internal node only holds keys; it
 * does not hold dictionary pairs.
 */
class InternalNode extends Node {
    int maxDegree;
    int minDegree;
    int degree; // the number of children (not keys!!)
    InternalNode leftSibling;
    InternalNode rightSibling;
    String[] keys;
    Node[] childPointers;

    /**
     * This method appends 'pointer' to the end of the childPointers
     * instance variable of the InternalNode object. The pointer can point to
     * an InternalNode object or a LeafNode object since the formal
     * parameter specifies a Node object.
     * 
     * @param pointer: Node pointer that is to be appended to the
     *                 childPointers list
     */
    void appendChildPointer(Node pointer) {
        this.childPointers[degree] = pointer;
        this.degree++;
    }

    /**
     * Given a Node pointer, this method will return the index of where the
     * pointer lies within the childPointers instance variable. If the pointer
     * can't be found, the method returns -1.
     * 
     * @param pointer: a Node pointer that may lie within the childPointers
     *                 instance variable
     * @return the index of 'pointer' within childPointers, or -1 if
     *         'pointer' can't be found
     */
    int findIndexOfPointer(Node pointer) {
        for (int i = 0; i < childPointers.length; i++) {
            if (childPointers[i] == pointer) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Given a pointer to a Node object and an string index, this method
     * inserts the pointer at the specified index within the childPointers
     * instance variable. As a result of the insert, some pointers may be
     * shifted to the right of the index.
     * 
     * @param pointer: the Node pointer to be inserted
     * @param index:   the index at which the insert is to take place
     */
    void insertChildPointer(Node pointer, int index) {
        for (int i = degree - 1; i >= index; i--) {
            childPointers[i + 1] = childPointers[i];
        }
        this.childPointers[index] = pointer;
        this.degree++;
    }

    /**
     * This simple method determines if the InternalNode is considered overfull,
     * i.e. the InternalNode object's current degree is one more than the
     * specified maximum.
     * 
     * @return a boolean indicating if the InternalNode is overfull
     */
    boolean isOverfull() {
        return this.degree == maxDegree + 1;
    }

    /**
     * Given a pointer to a Node object, this method inserts the pointer to
     * the beginning of the childPointers instance variable.
     * 
     * @param pointer: the Node object to be prepended within childPointers
     */
    void prependChildPointer(Node pointer) {
        for (int i = degree - 1; i >= 0; i--) {
            childPointers[i + 1] = childPointers[i];
        }
        this.childPointers[0] = pointer;
        this.degree++;
    }

    /**
     * This method sets keys[index] to null. This method is used within the
     * parent of a merging, deficient LeafNode.
     * 
     * @param index: the location within keys to be set to null
     */
    void removeKey(int index) {
        this.keys[index] = null;
    }

    /**
     * This method sets childPointers[index] to null and additionally
     * decrements the current degree of the InternalNode.
     * 
     * @param index: the location within childPointers to be set to null
     */
    void removePointer(int index) {
        this.childPointers[index] = null;
        this.degree--;
    }

    /**
     * This method removes 'pointer' from the childPointers instance
     * variable and decrements the current degree of the InternalNode. The
     * index where the pointer node was assigned is set to null.
     * 
     * @param pointer: the Node pointer to be removed from childPointers
     */
    void removePointer(Node pointer) {
        for (int i = 0; i < childPointers.length; i++) {
            if (childPointers[i] == pointer) {
                this.childPointers[i] = null;
            }
        }
        this.degree--;
    }

    /**
     * Constructor
     * 
     * @param m:    the max degree of the InternalNode
     * @param keys: the list of keys that InternalNode is initialized with
     */
    InternalNode(int m, String[] keys) {
        this.maxDegree = m;
        this.minDegree = (int) Math.ceil(m / 2.0);
        this.degree = 0;
        this.keys = keys;
        this.childPointers = new Node[this.maxDegree + 1];
    }

    /**
     * Constructor
     * 
     * @param m:        the max degree of the InternalNode
     * @param keys:     the list of keys that InternalNode is initialized with
     * @param pointers: the list of pointers that InternalNode is initialized with
     */
    InternalNode(int m, String[] keys, Node[] pointers) {
        this.maxDegree = m;
        this.minDegree = (int) Math.ceil(m / 2.0);
        this.degree = Search.linearNullSearch(pointers);
        this.keys = keys;
        this.childPointers = pointers;
    }
}
