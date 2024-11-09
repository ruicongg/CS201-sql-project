// LSMTree.java
package edu.smu.smusql;

import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.parser.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LSMBloom implements StorageInterface {
    private static final int MAX_MEMTABLE_SIZE = 1000;
    private static final int BLOOM_FILTER_SIZE = 10000;

    private final ConcurrentMap<String, List<RowEntry>> memTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<RowEntry>> ssTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, BloomFilter> bloomFilters = new ConcurrentHashMap<>();

    @Override
    public boolean tableExists(String tableName) {
        return memTable.containsKey(tableName) || ssTable.containsKey(tableName);
    }

    @Override
    public void create(Create create) {
        String tableName = create.getTablename();
        if (tableExists(tableName)) {
            throw new IllegalStateException("Table " + tableName + " already exists");
        }
        memTable.put(tableName, new ArrayList<>());
        bloomFilters.put(tableName, new BloomFilter(BLOOM_FILTER_SIZE));
    }

    @Override
    public void insert(Insert insert) {
        String tableName = insert.getTablename();
        List<RowEntry> memTableRows = memTable.get(tableName);
        
        RowEntry newRow = new RowEntry();
        List<String> values = insert.getValues();
        List<String> columns = getColumns(tableName);

        for (int i = 0; i < columns.size(); i++) {
            newRow.addOrUpdateValue(columns.get(i), values.get(i));
        }
        memTableRows.add(newRow);

        // Add to Bloom filter
        bloomFilters.get(tableName).add(newRow.getValue("id"));

        // If memtable is full, move data into ssTable
        if (memTableRows.size() >= MAX_MEMTABLE_SIZE) {
            compact(tableName);
        }
    }

    @Override
    public int delete(Delete delete) {
        String tableName = delete.getTablename();
        List<RowEntry> memTableRows = memTable.get(tableName);
        List<WhereCondition> conditions = delete.getConditions();
        
        int[] deletedCount = {0};  // Single-element array as a mutable counter
        memTableRows.removeIf(row -> {
            boolean toDelete = row.evaluateAllConditions(conditions);
            if (toDelete) {
                deletedCount[0]++;  // Increment the counter
            }
            return toDelete;
        });
        return deletedCount[0];
    }

    @Override
    public List<RowEntry> select(Select select) {
        String tableName = select.getTablename();
        List<WhereCondition> conditions = select.getConditions();
        List<RowEntry> result = new ArrayList<>();

        // Scan memTable
        if (memTable.containsKey(tableName)) {
            for (RowEntry row : memTable.get(tableName)) {
                if (row.evaluateAllConditions(conditions)) {
                    result.add(row);
                }
            }
        }

        // Check if there is an 'id' condition to use with the Bloom filter
        String idConditionValue = null;
        for (WhereCondition condition : conditions) {
            if (condition.getColumn().equals("id")) { // Check if condition is on 'id' column
                idConditionValue = condition.getValue(); // Get the value to check in the Bloom filter
                break;
            }
        }

        // Use Bloom filter to check if a scan in ssTable is necessary
        if (ssTable.containsKey(tableName) && (idConditionValue == null || bloomFilters.get(tableName).mightContain(idConditionValue))) {
            for (RowEntry row : ssTable.get(tableName)) {
                if (row.evaluateAllConditions(conditions)) {
                    result.add(row);
                }
            }
        }

        return result;
    }

    @Override
    public int update(Update update) {
        String tableName = update.getTablename();
        List<RowEntry> memTableRows = memTable.get(tableName);
        List<WhereCondition> conditions = update.getConditions();
        
        int updatedCount = 0;
        for (RowEntry row : memTableRows) {
            if (row.evaluateAllConditions(conditions)) {
                row.addOrUpdateValue(update.getColumnname(), update.getValue());
                updatedCount++;
            }
        }
        return updatedCount;
    }

    @Override
    public List<String> getColumns(String tableName) {
        // Assume columns r predefined for each table
        return switch (tableName) {
            case "students" -> List.of("id", "name", "age", "gpa");
            default -> Collections.emptyList();
        };
    }

    @Override
    public int getColumnCount(String tableName) {
        return getColumns(tableName).size();
    }

    private void compact(String tableName) {
        List<RowEntry> memTableRows = memTable.get(tableName);
        List<RowEntry> ssTableRows = ssTable.computeIfAbsent(tableName, k -> new ArrayList<>());

        // Simple compaction: merge memTable into ssTable and clear memTable
        ssTableRows.addAll(memTableRows);
        memTableRows.clear();

        // Sort ssTable 
        ssTableRows.sort((r1, r2) -> {
            String id1 = r1.getValue("id");
            String id2 = r2.getValue("id");
            return id1.compareTo(id2);
        });
    }
}
