// LSMFQ.java
package edu.smu.smusql;

import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.parser.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LSMFQ implements StorageInterface {

    private static final int MAX_MEMTABLE_SIZE = 1000;

    private final ConcurrentMap<String, List<RowEntry>> memTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<RowEntry>> ssTable = new ConcurrentHashMap<>();
    private final Map<String, List<String>> tableColumns = new ConcurrentHashMap<>();

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
        // Store the columns for this table
        tableColumns.put(tableName, create.getColumns());
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

        // Scan memTable and ssTable
        if (memTable.containsKey(tableName)) {
            for (RowEntry row : memTable.get(tableName)) {
                if (row.evaluateAllConditions(conditions)) {
                    result.add(row);
                }
            }
        }
        if (ssTable.containsKey(tableName)) {
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
        // Retrieve columns dynamically from tableColumns
        return tableColumns.getOrDefault(tableName, Collections.emptyList());
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
