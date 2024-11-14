package edu.smu.smusql.table;

import edu.smu.smusql.bst.BinarySearchTree;
import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.parser.*;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

public class BSTStorage implements StorageInterface {
    private final Map<String, BinarySearchTree> tables = new HashMap<>();

    @Override
    public void create(Create create) {
        String tableName = create.getTablename();
        if (tables.containsKey(tableName)) {
            throw new IllegalStateException("Table " + tableName + " already exists");
        }
        tables.put(tableName, new BinarySearchTree());
    }

    @Override
    public void insert(Insert insert) {
        String tableName = insert.getTablename();
        BinarySearchTree bst = tables.get(tableName);
        if (bst == null) {
            throw new IllegalStateException("Table " + tableName + " does not exist");
        }
        Map<String, String> row = createRowMap(insert.getColumns(), insert.getValues());
        bst.insert(row, "id");
    }

    @Override
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    @Override
    public int getColumnCount(String tableName) {
        // Assuming all tables have the same columns
        return tables.get(tableName).getAllRows().get(0).size();
    }

    @Override
    public List<String> getColumns(String tableName) {
        // Assuming all tables have the same columns
        return new ArrayList<>(tables.get(tableName).getAllRows().get(0).keySet());
    }

    @Override
    public int delete(Delete delete) {
        String tableName = delete.getTablename();
        BinarySearchTree bst = tables.get(tableName);
        if (bst == null) {
            throw new IllegalStateException("Table " + tableName + " does not exist");
        }
        List<RowEntry> rows = bst.getAllRows();
        int initialSize = rows.size();
        for (RowEntry row : rows) {
            if (row.evaluateAllConditions(delete.getConditions())) {
                bst.delete(row.getValue("id"), "id");
            }
        }
        return initialSize - bst.getAllRows().size();
    }

    @Override
    public List<RowEntry> select(Select select) {
        String tableName = select.getTablename();
        BinarySearchTree bst = tables.get(tableName);
        if (bst == null) {
            throw new IllegalStateException("Table " + tableName + " does not exist");
        }
        return bst.getAllRows();
    }

    @Override
    public int update(Update update) {
        String tableName = update.getTablename();
        BinarySearchTree bst = tables.get(tableName);
        if (bst == null) {
            throw new IllegalStateException("Table " + tableName + " does not exist");
        }
        List<RowEntry> rows = bst.getAllRows();
        int updatedCount = 0;
        for (RowEntry row : rows) {
            if (row.evaluateAllConditions(update.getConditions())) {
                row.setValue(update.getColumnname(), update.getValue());
                updatedCount++;
            }
        }
        return updatedCount;
    }

    private Map<String, String> createRowMap(List<String> columns, List<String> values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }
        return row;
    }
}