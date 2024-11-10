package edu.smu.smusql.lsm;

import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.parser.WhereCondition;
import java.util.*;

public class LSMTree {
    private static final int MEMTABLE_LIMIT = 5;  // Threshold for flushing MemTable to SSTable
    private TreeMap<String, List<RowEntry>> memTable;      // In-memory storage
    private List<TreeMap<String, List<RowEntry>>> ssTables;  // Immutable SSTables

    public LSMTree() {
        this.memTable = new TreeMap<>();
        this.ssTables = new ArrayList<>();
    }

    public void add(String key, RowEntry value) {
        memTable.putIfAbsent(key, new ArrayList<>());
        memTable.get(key).add(value);
        if (memTable.size() >= MEMTABLE_LIMIT) {
            flushMemTableToSSTable();
        }
    }

    // need to fix later on, this gets all RowEntry with the primary key
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

    // need to fix later on, this removes all RowEntry with the primary key
    public void remove(String key) {
        memTable.put(key, null);  // `null` signifies deletion
        if (memTable.size() >= MEMTABLE_LIMIT) {
            flushMemTableToSSTable();
        }
    }

    // Flush MemTable to a new SSTable and reset it
    private void flushMemTableToSSTable() {
        // Create a copy of the current MemTable and add it as a new SSTable
        TreeMap<String, List<RowEntry>> ssTable = new TreeMap<>(memTable);
        ssTables.add(ssTable);

        // Clear the MemTable for new entries
        memTable.clear();
        compactSSTables();  // Optional: Trigger compaction after flushing
    }

    // Compact SSTables to reduce levels and apply tombstones
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


    public List<RowEntry> getEntriesFromCondition(String operator, String value) {
        return switch (operator.toUpperCase()) {
            case ">" -> getEntriesWithKeyGreaterThan(value, false);
            case "<" -> getEntriesWithKeyLessThan(value, false);
            case ">=" -> getEntriesWithKeyGreaterThan(value, true);
            case "<=" -> getEntriesWithKeyLessThan(value, true);
            default -> null;
        };
    }

    public List<RowEntry> getEntriesWithKeyLessThan(String key, boolean inclusive) {
        List<RowEntry> result = new ArrayList<>();
    
        // Get all entries in memTable with keys less than (or equal to) the given key
        NavigableMap<String, List<RowEntry>> subMap = memTable.headMap(key, inclusive); // Use the flag here
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

    public List<RowEntry> getEntriesWithKeyGreaterThan(String key, boolean inclusive) {
        List<RowEntry> result = new ArrayList<>();
    
        // Get all entries in memTable with keys greater than (or equal to) the given key
        NavigableMap<String, List<RowEntry>> subMap = memTable.tailMap(key, inclusive); // Use the flag here
        for (List<RowEntry> entryList : subMap.values()) {
            entryList.removeIf(RowEntry::isDeleted);
            result.addAll(entryList);
        }
    
        // Repeat the process for all SSTables (most recent first)
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            TreeMap<String, List<RowEntry>> ssTable = ssTables.get(i);
            subMap = ssTable.tailMap(key, inclusive);
            for (List<RowEntry> entryList : subMap.values()) {
                for (RowEntry rowEntry: entryList) {
                }
                entryList.removeIf(RowEntry::isDeleted);
                result.addAll(entryList);
            }
        }
    
        return result;
    }

    // Print the the LSM Tree
    public void printTree() {
        System.out.println("MemTable: " + memTable);
        System.out.println("SSTables:");
        for (TreeMap<String, List<RowEntry>> ssTable : ssTables) {
            System.out.println("  " + ssTable);
        }
    }
}
