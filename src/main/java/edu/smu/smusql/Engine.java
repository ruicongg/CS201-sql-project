package edu.smu.smusql;

import java.util.*;
import java.util.stream.Collectors;

import edu.smu.smusql.parser.*;
public class Engine {
    // v1: uses hash map of tableName to Table
    private final Map<String, Table> tables = new HashMap<>();

    public String executeSQL(String query) {
        /*
         * Basic Input Validation
         */
        try {
            if (query == null || query.length() == 0) {
                throw new InvalidCommandException("ERROR: No command found");
            }
            Object parsedStatement = Parser.parseStatement(query);
            
            if (parsedStatement instanceof Create create) {
                return create(create);
            } else if (parsedStatement instanceof Insert insert) {
                return insert(insert);
            } else if (parsedStatement instanceof Select select) {
                return select(select);
            } else if (parsedStatement instanceof Update update) {
                return update(update);
            } else if (parsedStatement instanceof Delete delete) {
                return delete(delete);
            } else {
                throw new InvalidCommandException("ERROR:Unsupported command type");
            }
        } catch (InvalidCommandException e) {
            return e.getMessage();
        }
    }

    public String insert(Insert insert) {
        

        String tableName = insert.getTablename();
        Table table = getTableOrThrow(tableName);

        List<String> values = insert.getValues();
        List<String> columns = table.getColumns();

        if (values.size() != columns.size()) {
            throw new InvalidCommandException("ERROR: Column count doesn't match value count");
        }

        table.addRow(createRowMap(columns, values));
        return "Row inserted into " + tableName;
    }

    public String delete(Delete delete) {
        
        String tableName = delete.getTablename();
        Table table = getTableOrThrow(tableName);
        List<WhereCondition> conditions = delete.getConditions();
        List<RowEntry> rows = table.getRows();
        List<RowEntry> remainingRows = new ArrayList<>();
        int deletedCount = 0;

        for (RowEntry row : rows) {
            if (!evaluateConditions(conditions, row)) {
                remainingRows.add(row);
            } else {
                deletedCount++;
            }
        }

        table.setRows(remainingRows);
        return "Rows deleted from " + tableName + ". " + deletedCount + " rows affected.";
    }

    public String select(Select select) {
    

        String tableName = select.getTablename();
        Table table = getTableOrThrow(tableName);

        List<String> columns = table.getColumns();
        List<RowEntry> rows = processWhereConditions(table, select.getConditions());
        return formatTableOutput(columns, rows);
    }

    public String update(Update update) {


        String tableName = update.getTablename();
        Table table = getTableOrThrow(tableName);

        String setColumn = update.getColumnname();
        String newValue = update.getValue();

        if (!table.getColumns().contains(setColumn)) {
            throw new InvalidCommandException("ERROR: Column not found");
        }

        List<RowEntry> rows = processWhereConditions(table, update.getConditions());
        int updatedCount = 0;
        for (RowEntry row : rows) {
            row.addValue(setColumn, newValue);
            updatedCount++;
        }
        return String.format("Table %s updated. %d rows affected.", tableName, updatedCount);
    }

    public String create(Create create) {
    
        String tableName = create.getTablename();
        Table existingTable = tables.get(tableName);
        if (existingTable != null) {
            throw new InvalidCommandException("ERROR: Table already exists");
        }

        List<String> columns = create.getColumns();

        Table newTable = new Table(tableName, columns);
        tables.put(tableName, newTable);

        return "Table " + tableName + " created";
    }

    /*
     * HELPER METHODS
     */

    private RowEntry createRowMap(List<String> columns, List<String> values) {
        RowEntry row = new RowEntry();
        for (int i = 0; i < columns.size(); i++) {
            row.addValue(columns.get(i), values.get(i));
        }
        return row;
    }

    private boolean evaluateConditions(List<WhereCondition> conditions, RowEntry row) {
        return conditions.stream().allMatch(condition -> condition.evaluate(row));
    }

    private String formatTableOutput(List<String> columns, List<RowEntry> rows) {
        StringBuilder result = new StringBuilder();
        // Headers
        result.append(String.join("\t", columns))
              .append("\n");
        
        // Rows

        for (RowEntry row : rows) {
            result.append(row.getValue(columns.get(0)));
        }
        for (RowEntry row : rows) {
            for (int i = 1; i < columns.size(); i++) {
                result.append("\t");
                result.append(row.getValue(columns.get(i)));
            }
            result.append("\n");
        }
        
        return result.toString();
    }

    private Table getTableOrThrow(String tableName) {
        return Optional.ofNullable(tables.get(tableName))
                      .orElseThrow(() -> new InvalidCommandException("ERROR: Table not found"));
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
                       .filter(row -> evaluateConditions(conditions, row))
                       .toList();
        }

        // Use first equality condition for initial index lookup
        WhereCondition firstCondition = equalityConditions.get(0);
        List<RowEntry> matchingRows = table.findRowsByColumnValue(
            firstCondition.getColumn(), 
            firstCondition.getValue()
        );

        // If there is a second equality condition, apply it
        if (equalityConditions.size() == 2) {
            WhereCondition secondCondition = equalityConditions.get(1);
            matchingRows = matchingRows.stream()
                .filter(row -> table.findRowsByColumnValue(secondCondition.getColumn(), secondCondition.getValue()).contains(row))
                .toList();
        }

        // Apply any remaining non-equality conditions
        return matchingRows.stream()
                          .filter(row -> evaluateConditions(remainingConditions, row))
                          .toList();
    }

}
