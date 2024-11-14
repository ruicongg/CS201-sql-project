package edu.smu.smusql.table;

import java.util.*;
import java.util.stream.Stream;
import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.parser.*;
import edu.smu.smusql.lsm.*;


/**
 * An implementation of the StorageInterface using an LSM Tree for storing and querying table data.
 * This class supports basic operations such as insert, delete, update, and select, 
 * along with creating tables.
 */
public class LSMStorage implements StorageInterface {
    private final Map<String, Table> tables = new HashMap<>();  // Stores table names mapped to Table objects.
    private final Map<String, Map<String, LSMTree>> lsmTrees = new HashMap<>();  // Stores LSM Trees by table and column.

    /**
     * Inserts a new row into the specified table and updates the relevant LSM Trees.
     *
     * @param insert The Insert object containing table name and row data.
     */
    @Override
    public void insert(Insert insert) {
        Table table = tables.get(insert.getTablename());
        List<String> columnNames = table.getColumns();
        RowEntry rowEntry = createRowMap(columnNames, insert.getValues());
        table.addRow(rowEntry);

        Map<String, LSMTree> columnsInTable = lsmTrees.get(insert.getTablename());
        for (int i = 0; i < columnNames.size(); i++) {
            String currentColumn = columnNames.get(i);
            LSMTree lsmTreeForColumn = columnsInTable.get(currentColumn);
            lsmTreeForColumn.add(insert.getValues().get(i), rowEntry);
        }
    }

    /**
     * Checks if a table with the specified name exists.
     *
     * @param tableName The name of the table.
     * @return True if the table exists, false otherwise.
     */
    @Override
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    /**
     * Returns the number of columns in the specified table.
     *
     * @param tableName The name of the table.
     * @return The number of columns in the table.
     */
    @Override
    public int getColumnCount(String tableName) {
        return tables.get(tableName).getColumns().size();
    }

    /**
     * Retrieves the list of column names for the specified table.
     *
     * @param tableName The name of the table.
     * @return A list of column names.
     */
    @Override
    public List<String> getColumns(String tableName) {
        return tables.get(tableName).getColumns();
    }

    /**
     * Deletes rows from the specified table based on the given conditions.
     *
     * @param delete The Delete object containing table name and conditions.
     * @return The number of rows deleted.
     */
    @Override
    public int delete(Delete delete) {
        Table table = tables.get(delete.getTablename());
        List<RowEntry> rows = table.getRows();
        int deletedCount = 0;

        Iterator<RowEntry> rowsIterator = rows.iterator();
        while (rowsIterator.hasNext()) {
            RowEntry row = rowsIterator.next();
            if (row.evaluateAllConditions(delete.getConditions())) {
                row.setDeleted();  // Mark the row as deleted.
                rowsIterator.remove();
                deletedCount++;
            }
        }

        table.setRows(rows);
        return deletedCount;
    }

    /**
     * Selects rows from a table based on the provided conditions.
     *
     * @param select The Select object containing table name and conditions.
     * @return A list of RowEntry objects that match the conditions.
     */
    @Override
    public List<RowEntry> select(Select select) {
        return processWhereConditions(tables.get(select.getTablename()), select.getConditions());
    }

    /**
     * Updates rows in a table based on specified conditions and updates the specified column.
     *
     * @param update The Update object containing table name, column name, value, and conditions.
     * @return The number of rows updated.
     */
    @Override
    public int update(Update update) {
        List<RowEntry> rows = processWhereConditions(tables.get(update.getTablename()), update.getConditions());
        int updatedCount = 0;
        for (RowEntry row : rows) {
            row.addOrUpdateValue(update.getColumnname(), update.getValue());
            updatedCount++;
        }
        return updatedCount;
    }

    /**
     * Creates a new table with the specified name and columns.
     *
     * @param create The Create object containing table name and columns.
     */
    @Override
    public void create(Create create) {
        Table newTable = new Table(create.getTablename(), create.getColumns());
        tables.put(create.getTablename(), newTable);

        Map<String, LSMTree> columnsInTable = new HashMap<>();
        for (String column : create.getColumns()) {
            columnsInTable.put(column, new LSMTree());
        }
        lsmTrees.put(create.getTablename(), columnsInTable);
    }

    /*
     * HELPER METHODS
     */

    /**
     * Creates a RowEntry object from a list of column names and corresponding values.
     *
     * @param columns The list of column names.
     * @param values  The list of values for the columns.
     * @return A RowEntry object representing a row in the table.
     */
    private RowEntry createRowMap(List<String> columns, List<String> values) {
        RowEntry row = new RowEntry();
        for (int i = 0; i < columns.size(); i++) {
            row.addOrUpdateValue(columns.get(i), values.get(i));
        }
        return row;
    }

    /**
     * Processes the conditions provided for a select operation, using both equality
     * and non-equality conditions to filter rows.
     *
     * @param table      The table to search.
     * @param conditions The conditions to apply.
     * @return A list of RowEntry objects that match the conditions.
     */
    private List<RowEntry> processWhereConditions(Table table, List<WhereCondition> conditions) {
        if (conditions.isEmpty()) {
            return table.getRows();
        }

        List<WhereCondition> equalityConditions = new ArrayList<>();
        List<WhereCondition> remainingConditions = new ArrayList<>();
        for (WhereCondition condition : conditions) {
            if (condition.getOperator().equals("=")) {
                equalityConditions.add(condition);
            } else {
                remainingConditions.add(condition);
            }
        }

        List<RowEntry> matchingRows = null;

        // Process the first non-equality condition using LSM Trees if applicable
        if (remainingConditions.size() >= 1) {
            String tableName = table.getName();
            String condition1Column = remainingConditions.get(0).getColumn();
            LSMTree condition1LSMTree = lsmTrees.get(tableName).get(condition1Column);

            String condition1Operator = remainingConditions.get(0).getOperator();
            String condition1Value = remainingConditions.get(0).getValue();

            matchingRows = condition1LSMTree.getEntriesFromCondition(condition1Operator, condition1Value);
        }

        // Process equality conditions using table's row lookup
        if (equalityConditions.size() >= 1) {
            WhereCondition firstCondition = equalityConditions.get(0);
            matchingRows = table.findRowsByColumnValue(
                    firstCondition.getColumn(),
                    firstCondition.getValue());
        }

        // Handle a second non-equality condition with logical OR/AND
        if (remainingConditions.size() == 2) {
            boolean isOr = conditions.get(0).getLogicalOperator().equals("OR");
            String tableName = table.getName();
            String condition2Column = remainingConditions.get(1).getColumn();
            LSMTree condition2LSMTree = lsmTrees.get(tableName).get(condition2Column);

            String condition2Operator = remainingConditions.get(1).getOperator();
            String condition2Value = remainingConditions.get(1).getValue();

            if (isOr) {
                Set<RowEntry> matchingRowsSet = new LinkedHashSet<>(matchingRows);  // Use a Set to avoid duplicates
                matchingRowsSet.addAll(condition2LSMTree.getEntriesFromCondition(condition2Operator, condition2Value));
                matchingRows = new ArrayList<>(matchingRowsSet);
            } else {
                matchingRows.retainAll(condition2LSMTree.getEntriesFromCondition(condition2Operator, condition2Value));
            }
        }

        // Apply a second equality condition if present
        if (equalityConditions.size() == 2) {
            boolean isOr = conditions.get(0).getLogicalOperator().equals("OR");
            WhereCondition secondCondition = equalityConditions.get(1);
            if (isOr) {
                List<RowEntry> equalityRow = table.findRowsByColumnValue(
                        secondCondition.getColumn(),
                        secondCondition.getValue());
                matchingRows = Stream.concat(
                        matchingRows.stream(),
                        equalityRow.stream()
                )
                .distinct()  // Remove duplicates if needed
                .toList();
            } else {
                matchingRows = matchingRows.stream()
                        .filter(row -> table.findRowsByColumnValue(secondCondition.getColumn(), secondCondition.getValue())
                                .contains(row))
                        .toList();
            }
        }

        return matchingRows;
    }
}