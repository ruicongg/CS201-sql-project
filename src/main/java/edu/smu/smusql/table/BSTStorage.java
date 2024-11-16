package edu.smu.smusql.table;

import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.parser.*;
import edu.smu.smusql.bst.BinarySearchTree;

import java.util.*;

public class BSTStorage implements StorageInterface {
    // Map to hold BinarySearchTree instances for each table
    private final Map<String, BinarySearchTree> bstMap = new HashMap<>();
    // Map to hold Table metadata
    private final Map<String, Table> tables = new HashMap<>();

    /**
     * Creates a new table and initializes its BinarySearchTree.
     *
     * @param create The Create object containing table name and columns.
     */
    @Override
    public void create(Create create) {
        String tableName = create.getTablename();
        List<String> columns = create.getColumns();

        // Initialize the Table
        Table newTable = new Table(tableName, columns);
        tables.put(tableName, newTable);

        // Initialize the Binary Search Tree for this table
        BinarySearchTree bst = new BinarySearchTree();
        bstMap.put(tableName, bst);
    }

    /**
     * Inserts a new row into the specified table's BST.
     *
     * @param insert The Insert object containing table name and row data.
     */
    @Override
    public void insert(Insert insert) {
        String tableName = insert.getTablename();
        BinarySearchTree bst = bstMap.get(tableName);
        Table table = tables.get(tableName);

        // Create RowEntry from Insert values
        RowEntry rowEntry = createRowEntry(table.getColumns(), insert.getValues());

        // Insert into BST
        String primaryKey = table.getColumns().get(0); // Assuming first column is primary key
        bst.insert(rowEntry, primaryKey);

        // Add row to Table metadata
        table.addRow(rowEntry);
    }

    /**
     * Deletes rows from the specified table's BST based on conditions.
     *
     * @param delete The Delete object containing table name and conditions.
     * @return The number of rows deleted.
     */
    @Override
    public int delete(Delete delete) {
        String tableName = delete.getTablename();
        BinarySearchTree bst = bstMap.get(tableName);
        Table table = tables.get(tableName);
        List<WhereCondition> conditions = delete.getConditions();
        String primaryKey = table.getColumns().get(0); // Assuming first column is primary key

        // Find rows matching conditions
        List<RowEntry> rowsToDelete = filterRows(table.getRows(), conditions);

        int deletedCount = 0;
        for (RowEntry row : rowsToDelete) {
            String key = row.getValue(primaryKey);
            bst.delete(key, primaryKey);
            deletedCount++;
        }

        // Remove rows from Table metadata
        table.setRows(removeRows(table.getRows(), rowsToDelete));

        return deletedCount;
    }

    /**
     * Selects and retrieves rows from the specified table's BST based on
     * conditions.
     *
     * @param select The Select object containing table name and conditions.
     * @return A list of RowEntry objects that match the conditions.
     */
    @Override
    public List<RowEntry> select(Select select) {
        String tableName = select.getTablename();
        BinarySearchTree bst = bstMap.get(tableName);
        Table table = tables.get(tableName);
        List<WhereCondition> conditions = select.getConditions();
        String primaryKey = table.getColumns().get(0); // Assuming first column is primary key

        // If no conditions, return all rows
        if (conditions.isEmpty()) {
            return bst.getAllRows();
        }

        // Find rows matching conditions
        return filterRows(table.getRows(), conditions);
    }

    /**
     * Updates rows in the specified table's BST based on conditions.
     *
     * @param update The Update object containing table name, column name, value,
     *               and conditions.
     * @return The number of rows updated.
     */
    @Override
    public int update(Update update) {
        String tableName = update.getTablename();
        BinarySearchTree bst = bstMap.get(tableName);
        Table table = tables.get(tableName);
        List<WhereCondition> conditions = update.getConditions();
        String columnName = update.getColumnname();
        String newValue = update.getValue();
        String primaryKey = table.getColumns().get(0); // Assuming first column is primary key

        // Find rows matching conditions
        List<RowEntry> rowsToUpdate = filterRows(table.getRows(), conditions);
        int updatedCount = 0;

        for (RowEntry row : rowsToUpdate) {
            String oldKey = row.getValue(primaryKey);

            // If primary key is being updated, delete the node first
            if (columnName.equals(primaryKey)) {
                bst.delete(oldKey, primaryKey); // Remove old node
            }

            // Update the row
            row.addOrUpdateValue(columnName, newValue);

            // If primary key is updated, re-insert the updated row
            if (columnName.equals(primaryKey)) {
                bst.insert(row, primaryKey); // Insert updated node
            }

            updatedCount++;
        }

        return updatedCount;
    }

    /**
     * Checks if a table exists.
     *
     * @param tableName The name of the table.
     * @return True if the table exists, false otherwise.
     */
    @Override
    public boolean tableExists(String tableName) {
        // System.out.println("[DEBUG] Checking existence of table '" + tableName + "':
        // " + tables.containsKey(tableName));
        return tables.containsKey(tableName);
    }

    /**
     * Gets the number of columns in a table.
     *
     * @param tableName The name of the table.
     * @return The number of columns.
     */
    @Override
    public int getColumnCount(String tableName) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist.");
        }
        return tables.get(tableName).getColumns().size();
    }

    /**
     * Retrieves the list of column names for a table.
     *
     * @param tableName The name of the table.
     * @return A list of column names.
     */
    @Override
    public List<String> getColumns(String tableName) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist.");
        }
        return tables.get(tableName).getColumns();
    }

    /*
     * HELPER METHODS
     */

    /**
     * Creates a RowEntry from lists of columns and values.
     *
     * @param columns The list of column names.
     * @param values  The list of corresponding values.
     * @return A RowEntry object.
     */
    private RowEntry createRowEntry(List<String> columns, List<String> values) {
        RowEntry row = new RowEntry();
        for (int i = 0; i < columns.size(); i++) {
            row.addOrUpdateValue(columns.get(i), values.get(i));
        }
        return row;
    }

    /**
     * Filters rows based on provided conditions.
     *
     * @param rows       The list of rows to filter.
     * @param conditions The list of conditions to apply.
     * @return A list of rows that match the conditions.
     */
    private List<RowEntry> filterRows(List<RowEntry> rows, List<WhereCondition> conditions) {
        List<RowEntry> result = new ArrayList<>();
        for (RowEntry row : rows) {
            if (row.evaluateAllConditions(conditions)) {
                result.add(row);
            }
        }
        return result;
    }

    /**
     * Removes specified rows from the main list of rows.
     *
     * @param originalRows The original list of rows.
     * @param rowsToRemove The list of rows to remove.
     * @return A new list of rows after removal.
     */
    private List<RowEntry> removeRows(List<RowEntry> originalRows, List<RowEntry> rowsToRemove) {
        List<RowEntry> updatedRows = new ArrayList<>(originalRows);
        updatedRows.removeAll(rowsToRemove);
        return updatedRows;
    }
}
