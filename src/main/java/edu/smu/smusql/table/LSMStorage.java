package edu.smu.smusql.table;

import java.util.*;
import java.util.stream.Stream;

import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.parser.*;
import edu.smu.smusql.lsm.*;

// v1: uses hash map of tableName to Table
// v2: processes 2 equality where conditions to improve performance

public class LSMStorage implements StorageInterface {
    private final Map<String, Table> tables = new HashMap<>();
    // Table name -> Column Name -> LSMTree
    private final Map<String, Map<String, LSMTree>> lsmTrees = new HashMap<>();

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
            lsmTreeForColumn.printTree();
        }
    }

    @Override
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    @Override
    public int getColumnCount(String tableName) {
        return tables.get(tableName).getColumns().size();
    }

    @Override
    public List<String> getColumns(String tableName) {
        return tables.get(tableName).getColumns();
    }

    @Override
    public int delete(Delete delete) {
        Table table = tables.get(delete.getTablename());
        List<RowEntry> rows = table.getRows();
        List<RowEntry> remainingRows = new ArrayList<>();
        int deletedCount = 0;

        Iterator<RowEntry> rowsIterator = rows.iterator();
        while (rowsIterator.hasNext()) {
            RowEntry row = rowsIterator.next();
            if (row.evaluateAllConditions(delete.getConditions())) {

                // Set marker to show row is deleted
                row.setDeleted();
                rowsIterator.remove();
                deletedCount++;
            }
        }

        table.setRows(rows);

        return deletedCount;
    }

    @Override
    public List<RowEntry> select(Select select) {
        return processWhereConditions(tables.get(select.getTablename()), select.getConditions());
    }

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

    @Override
    public void create(Create create) {
        Table newTable = new Table(create.getTablename(), create.getColumns());
        tables.put(create.getTablename(), newTable);

        Map<String, LSMTree> columnsInTable = new HashMap<>();
        for (String column: create.getColumns()) {
            columnsInTable.put(column, new LSMTree());
        }
        lsmTrees.put(create.getTablename(), columnsInTable);
    }

    /*
     * HELPER METHODS
     */

    private RowEntry createRowMap(List<String> columns, List<String> values) {
        RowEntry row = new RowEntry();
        for (int i = 0; i < columns.size(); i++) {
            row.addOrUpdateValue(columns.get(i), values.get(i));
        }
        return row;
    }

    private List<RowEntry> processWhereConditions(Table table, List<WhereCondition> conditions) {
        if (conditions.isEmpty()) {
            return table.getRows();
        }

        // Get equality conditions that can use indices
        List<WhereCondition> equalityConditions = new ArrayList<>();

        // Using LSM tree for rest of conditions
        List<WhereCondition> remainingConditions =  new ArrayList<>();
        for (WhereCondition condition : conditions) {
            if (condition.getOperator().equals("=")) {
                equalityConditions.add(condition);
            } else {
                remainingConditions.add(condition);
            }
        }

        List<RowEntry> matchingRows = null;

        // if there are no equality conditions
        if (remainingConditions.size() >= 1) {
            String tableName = table.getName();
            String condition1Column = remainingConditions.get(0).getColumn();
            LSMTree condition1LSMTree = lsmTrees.get(tableName).get(condition1Column);

            String condition1Operator = remainingConditions.get(0).getOperator();
            String condition1Value = remainingConditions.get(0).getValue();

            matchingRows = condition1LSMTree.getEntriesFromCondition(condition1Operator, condition1Value);
            System.out.println(matchingRows);
        }

        if (equalityConditions.size() >= 1) {
            WhereCondition firstCondition = equalityConditions.get(0);
            matchingRows = table.findRowsByColumnValue(
                    firstCondition.getColumn(),
                    firstCondition.getValue());
            
        }

        
        

        if (remainingConditions.size() == 2) {
            boolean isOr = conditions.get(0).getLogicalOperator().equals("OR");
            String tableName = table.getName();
            String condition2Column = remainingConditions.get(1).getColumn();
            LSMTree condition2LSMTree = lsmTrees.get(tableName).get(condition2Column);

            String condition2Operator = remainingConditions.get(1).getOperator();
            String condition2Value = remainingConditions.get(1).getValue();
            
            if (isOr) {
                // Use a Set to keep entries distinct
                Set<RowEntry> matchingRowsSet = new LinkedHashSet<>(matchingRows); // Preserve insertion order
                matchingRowsSet.addAll(condition2LSMTree.getEntriesFromCondition(condition2Operator, condition2Value));

                // Convert the Set back to a List if needed
                matchingRows = new ArrayList<>(matchingRowsSet);
                
            } else {
                matchingRows.retainAll(condition2LSMTree.getEntriesFromCondition(condition2Operator, condition2Value));
            }
        }

        // If there is a second equality condition, apply it
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
                .distinct()  // Optional: remove duplicates if needed
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