package edu.smu.smusql.lsm;

import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.parser.WhereCondition;
import java.util.*;

/**
 * Represents a Log-Structured Merge Tree (LSM Tree) for managing and querying
 * large amounts of data with efficient range queries.
 */
public class LSMTree {
    private static final int MEMTABLE_LIMIT = 5;  // Threshold for flushing MemTable to SSTable
    private TreeMap<String, List<RowEntry>> memTable;  // In-memory storage
    private List<TreeMap<String, List<RowEntry>>> ssTables;  // Immutable SSTables

    /**
     * Initializes a new LSMTree instance with an empty MemTable and list of SSTables.
     */
    public LSMTree() {
        this.memTable = new TreeMap<>();
        this.ssTables = new ArrayList<>();
    }

    /**
     * Adds a new entry to the MemTable. If the MemTable exceeds its size limit,
     * it is flushed to a new SSTable.
     *
     * @param key   The key associated with the entry.
     * @param value The RowEntry to be added.
     */
    public void add(String key, RowEntry value) {
        memTable.putIfAbsent(key, new ArrayList<>());
        memTable.get(key).add(value);
        if (memTable.size() >= MEMTABLE_LIMIT) {
            flushMemTableToSSTable();
        }
    }

    /**
     * Retrieves all RowEntry objects associated with a given key.
     * The method searches the MemTable first, followed by SSTables from newest to oldest.
     * Not used since we only use the LSM for range queries
     *
     * @param key The key to search for.
     * @return A list of RowEntry objects associated with the key, or null if not found.
     */
    public List<RowEntry> get(String key) {
        // First check MemTable
        if (memTable.containsKey(key)) {
            return memTable.get(key);
        }

        // Check SSTables in reverse order (most recent first)
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            TreeMap<String, List<RowEntry>> ssTable = ssTables.get(i);
            if (ssTable.containsKey(key)) {
                return ssTable.get(key);
            }
        }

        return null;
    }

    /**
     * Marks all entries associated with a given key for deletion by placing
     * a tombstone (null) in the MemTable.
     * Not used since we only use the LSM for range queries
     *
     * @param key The key to remove.
     */
    public void remove(String key) {
        memTable.put(key, null);  // `null` signifies deletion
        if (memTable.size() >= MEMTABLE_LIMIT) {
            flushMemTableToSSTable();
        }
    }

    /**
     * Flushes the current MemTable to a new SSTable and resets the MemTable.
     */
    private void flushMemTableToSSTable() {
        // Create a copy of the current MemTable and add it as a new SSTable
        TreeMap<String, List<RowEntry>> ssTable = new TreeMap<>(memTable);
        ssTables.add(ssTable);

        // Clear the MemTable for new entries
        memTable.clear();
        compactSSTables();  // Optional: Trigger compaction after flushing
    }

    /**
     * Compacts all SSTables to reduce the number of levels and apply tombstone
     * deletions, creating a new compacted SSTable.
     */
    private void compactSSTables() {
        TreeMap<String, List<RowEntry>> newSSTable = new TreeMap<>();

        // Traverse SSTables from oldest to newest
        for (TreeMap<String, List<RowEntry>> ssTable : ssTables) {
            for (Map.Entry<String, List<RowEntry>> entry : ssTable.entrySet()) {
                String key = entry.getKey();
                List<RowEntry> value = entry.getValue();

                // If not a tombstone, put it into newSSTable
                if (value != null) {
                    newSSTable.putIfAbsent(key, new ArrayList<>());
                    newSSTable.get(key).addAll(value);
                } else {
                    newSSTable.remove(key);  // Remove deleted keys
                }
            }
        }

        // Clear existing SSTables and add the compacted version
        ssTables.clear();
        ssTables.add(newSSTable);
    }

    /**
     * Retrieves entries from the LSM Tree that match a specified condition.
     *
     * @param operator The comparison operator (e.g., ">", "<", ">=", "<=").
     * @param value    The value to compare against.
     * @return A list of RowEntry objects that match the condition.
     */
    public List<RowEntry> getEntriesFromCondition(String operator, String value) {
        return switch (operator.toUpperCase()) {
            case ">" -> getEntriesWithKeyGreaterThan(value, false);
            case "<" -> getEntriesWithKeyLessThan(value, false);
            case ">=" -> getEntriesWithKeyGreaterThan(value, true);
            case "<=" -> getEntriesWithKeyLessThan(value, true);
            default -> null;
        };
    }

    /**
     * Retrieves all entries with keys less than (or optionally equal to) the given key.
     *
     * @param key       The key to compare against.
     * @param inclusive If true, includes entries with the given key.
     * @return A list of RowEntry objects with keys less than the specified key.
     */
    public List<RowEntry> getEntriesWithKeyLessThan(String key, boolean inclusive) {
        List<RowEntry> result = new ArrayList<>();

        // Get all entries in memTable with keys less than (or equal to) the given key
        NavigableMap<String, List<RowEntry>> subMap = memTable.headMap(key, inclusive);
        for (List<RowEntry> entryList : subMap.values()) {
            entryList.removeIf(RowEntry::isDeleted);
            result.addAll(entryList);
        }

        // Repeat the process for all SSTables (most recent first)
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            TreeMap<String, List<RowEntry>> ssTable = ssTables.get(i);
            subMap = ssTable.headMap(key, inclusive);
            for (List<RowEntry> entryList : subMap.values()) {
                entryList.removeIf(RowEntry::isDeleted);
                result.addAll(entryList);
            }
        }

        return result;
    }

    /**
     * Retrieves all entries with keys greater than (or optionally equal to) the given key.
     *
     * @param key       The key to compare against.
     * @param inclusive If true, includes entries with the given key.
     * @return A list of RowEntry objects with keys greater than the specified key.
     */
    public List<RowEntry> getEntriesWithKeyGreaterThan(String key, boolean inclusive) {
        List<RowEntry> result = new ArrayList<>();

        // Get all entries in memTable with keys greater than (or equal to) the given key
        NavigableMap<String, List<RowEntry>> subMap = memTable.tailMap(key, inclusive);
        for (List<RowEntry> entryList : subMap.values()) {
            entryList.removeIf(RowEntry::isDeleted);
            result.addAll(entryList);
        }

        // Repeat the process for all SSTables (most recent first)
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            TreeMap<String, List<RowEntry>> ssTable = ssTables.get(i);
            subMap = ssTable.tailMap(key, inclusive);
            for (List<RowEntry> entryList : subMap.values()) {
                entryList.removeIf(RowEntry::isDeleted);
                result.addAll(entryList);
            }
        }

        return result;
    }

    /**
     * Prints the current state of the LSM Tree, including the MemTable and all SSTables.
     */
    public void printTree() {
        System.out.println("MemTable: " + memTable);
        System.out.println("SSTables:");
        for (TreeMap<String, List<RowEntry>> ssTable : ssTables) {
            System.out.println("  " + ssTable);
        }
    }
}