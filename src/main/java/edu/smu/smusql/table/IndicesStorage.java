package edu.smu.smusql.table;

import java.util.*;

import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.parser.*;

// v1: uses hash map of tableName to Table
// v2: processes 2 equality where conditions to improve performance

public class IndicesStorage implements StorageInterface {
    private final Map<String, Table> tables = new HashMap<>();

    @Override
    public void insert(Insert insert) {
        Table table = tables.get(insert.getTablename());
        table.addRow(createRowMap(table.getColumns(), insert.getValues()));
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

        for (RowEntry row : rows) {
            if (!row.evaluateAllConditions(delete.getConditions())) {
                remainingRows.add(row);
            } else {
                deletedCount++;
            }
        }

        table.setRows(remainingRows);
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
        List<WhereCondition> remainingConditions = new ArrayList<>();
        for (WhereCondition condition : conditions) {
            if (condition.getOperator().equals("=")) {
                equalityConditions.add(condition);
            } else {
                remainingConditions.add(condition);
            }
        }

        // if there are no equality conditions
        if (equalityConditions.isEmpty()) {
            return table.getRows().stream()
                    .filter(row -> row.evaluateAllConditions(conditions))
                    .toList();
        }

        // Use first equality condition for initial index lookup
        WhereCondition firstCondition = equalityConditions.get(0);
        List<RowEntry> matchingRows = table.findRowsByColumnValue(
                firstCondition.getColumn(),
                firstCondition.getValue());

        // If there is a second equality condition, apply it
        if (equalityConditions.size() == 2) {
            WhereCondition secondCondition = equalityConditions.get(1);
            matchingRows = matchingRows.stream()
                    .filter(row -> table.findRowsByColumnValue(secondCondition.getColumn(), secondCondition.getValue())
                            .contains(row))
                    .toList();
        }

        // Apply any remaining non-equality conditions
        return matchingRows.stream()
                .filter(row -> row.evaluateAllConditions(remainingConditions))
                .toList();
    }
}