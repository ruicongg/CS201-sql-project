package edu.smu.smusql.bplus;

import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.parser.*;

import java.util.*;

public class BPlusTreeStorage implements StorageInterface {
    private final Map<String, TableMetadata> tables = new HashMap<>();

    private static class TableMetadata {
        List<String> columns;
        Map<String, BPlusTree> columnIndices;

        TableMetadata(List<String> columns) {
            this.columns = columns;
            this.columnIndices = new HashMap<>();
            for (String column : columns) {
                columnIndices.put(column, new BPlusTree());
            }
        }
    }

    @Override
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    @Override
    public void create(Create create) {
        tables.put(create.getTablename(), new TableMetadata(create.getColumns()));
    }

    @Override
    public void insert(Insert insert) {
        TableMetadata table = tables.get(insert.getTablename());
        List<String> values = insert.getValues();
        
        // Create row entry
        RowEntry row = new RowEntry();
        for (int i = 0; i < table.columns.size(); i++) {
            row.addOrUpdateValue(table.columns.get(i), values.get(i));
        }

        // Insert into each column's B+ tree
        for (int i = 0; i < table.columns.size(); i++) {
            String column = table.columns.get(i);
            table.columnIndices.get(column).insert(values.get(i), row);
        }
    }

    @Override
    public List<String> getColumns(String tableName) {
        return tables.get(tableName).columns;
    }

    @Override
    public int getColumnCount(String tableName) {
        return tables.get(tableName).columns.size();
    }

    @Override
    public List<RowEntry> select(Select select) {
        TableMetadata table = tables.get(select.getTablename());
        List<WhereCondition> conditions = select.getConditions();

        if (conditions.isEmpty()) {
            // Return all rows if no conditions
            return table.columnIndices.get(table.columns.get(0)).getAllValues();
        }

        // Find equality conditions that we can use for index lookup
        Optional<WhereCondition> equalityCondition = conditions.stream()
                .filter(c -> c.getOperator().equals("="))
                .findFirst();

        List<RowEntry> result;
        if (equalityCondition.isPresent()) {
            // Use index for equality condition
            WhereCondition condition = equalityCondition.get();
            result = table.columnIndices.get(condition.getColumn())
                    .search(condition.getValue());
        } else {
            // Fall back to scanning all rows
            result = table.columnIndices.get(table.columns.get(0)).getAllValues();
        }

        // Apply remaining conditions
        return result.stream()
                .filter(row -> row.evaluateAllConditions(conditions))
                .toList();
    }

    @Override
    public int update(Update update) {
        TableMetadata table = tables.get(update.getTablename());
        List<RowEntry> rowsToUpdate = select(new Select(update.getTablename(), update.getConditions()));
        
        // Remove old index entries
        for (RowEntry row : rowsToUpdate) {
            for (String column : table.columns) {
                table.columnIndices.get(column).delete(row.getValue(column), row);
            }
        }

        // Update rows and reinsert
        for (RowEntry row : rowsToUpdate) {
            row.addOrUpdateValue(update.getColumnname(), update.getValue());
            for (String column : table.columns) {
                table.columnIndices.get(column).insert(row.getValue(column), row);
            }
        }

        return rowsToUpdate.size();
    }

    @Override
    public int delete(Delete delete) {
        TableMetadata table = tables.get(delete.getTablename());
        List<RowEntry> rowsToDelete = select(new Select(delete.getTablename(), delete.getConditions()));
        
        // Remove entries from all indices
        for (RowEntry row : rowsToDelete) {
            for (String column : table.columns) {
                table.columnIndices.get(column).delete(row.getValue(column), row);
            }
        }

        return rowsToDelete.size();
    }
} 