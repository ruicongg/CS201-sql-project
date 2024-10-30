package edu.smu.smusql;

import java.util.*;
import java.util.stream.Collectors;

public class Engine {
    // v1: uses hash map of tableName to Table
    private final Map<String, Table> tables = new HashMap<>();

    public String executeSQL(String query) {
        String[] tokens = query.trim().split("\\s+");
        String command = tokens[0].toUpperCase();

        return switch (command) {
            case "CREATE" -> create(tokens);
            case "INSERT" -> insert(tokens);
            case "SELECT" -> select(tokens);
            case "UPDATE" -> update(tokens);
            case "DELETE" -> delete(tokens);
            default -> "ERROR: Unknown command";
        };
    }

    public String insert(String[] tokens) {
        if (!tokens[1].toUpperCase().equals("INTO")) {
            return "ERROR: Invalid INSERT INTO syntax";
        }
        // Valid command:
        // INSERT INTO tableName VALUES (value1, value2, value3, ...)
        if (tokens.length < 5) {
            return "ERROR: Invalid INSERT INTO syntax";
        }

        String tableName = tokens[2];
        Table table = tables.get(tableName);
        if (table == null) {
            return "ERROR: Table not found";
        }

        String valueList = queryBetweenParentheses(tokens, 4);
        List<String> values = parseValuesToList(valueList);
        List<String> columns = table.getColumns();

        if (values.size() != columns.size()) {
            return "ERROR: Column count doesn't match value count";
        }

        Map<String, String> row = createRowMap(columns, values);
        table.addRow(row);
        return "Row inserted into " + tableName;
    }

    public String delete(String[] tokens) {
        if (!tokens[1].toUpperCase().equals("FROM")) {
            return "ERROR: Invalid DELETE syntax";
        }

        String tableName = tokens[2];
        Table table = tables.get(tableName);
        if (table == null) {
            return "ERROR: Table not found";
        }

        // Valid command:
        // DELETE FROM tableName WHERE conditions
        //                             ^ index 4
        List<WhereCondition> conditions = parseWhereClause(tokens, 4);
        List<Map<String, String>> rows = table.getRows();
        List<Map<String, String>> remainingRows = new ArrayList<>();
        int deletedCount = 0;

        for (Map<String, String> row : rows) {
            if (!evaluateConditions(conditions, row)) {
                remainingRows.add(row);
            } else {
                deletedCount++;
            }
        }

        table.setRows(remainingRows);
        return "Rows deleted from " + tableName + ". " + deletedCount + " rows affected.";
    }

    public String select(String[] tokens) {
        // Valid command:
        // SELECT * FROM tableName (only handles select * as per handout)
        if (!tokens[1].equals("*") || !tokens[2].toUpperCase().equals("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }

        String tableName = tokens[3];
        Table table = tables.get(tableName);
        if (table == null) {
            return "ERROR: Table not found";
        }

        List<String> columns = table.getColumns();
        List<Map<String, String>> rows;

        // If there's a WHERE clause
        if (tokens.length > 4 && tokens[4].toUpperCase().equals("WHERE")) {
            List<WhereCondition> conditions = parseWhereClause(tokens, 5);
            
            // Use index for the first condition if possible
            WhereCondition firstCondition = conditions.get(0);
            if (firstCondition.operator.equals("=")) {
                // Use index lookup for equality conditions
                rows = table.findRowsByColumnValue(firstCondition.column, firstCondition.value);
                // Filter remaining conditions
                rows = rows.stream()
                          .filter(row -> evaluateConditions(conditions, row))
                          .toList();
            } else {
                // Full scan for non-equality conditions
                rows = table.getRows().stream()
                           .filter(row -> evaluateConditions(conditions, row))
                           .toList();
            }
        } else {
            rows = table.getRows();
        }

        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n"); // Print column headers
        
        for (Map<String, String> row : rows) {
            for (String column : columns) {
                result.append(row.getOrDefault(column, "NULL")).append("\t");
            }
            result.append("\n");
        }

        return result.toString();
    }

    public String update(String[] tokens) {
        // Valid command:
        // UPDATE tableName SET column = value [WHERE conditions]
        if (tokens.length < 6 || !tokens[2].toUpperCase().equals("SET") || !tokens[4].equals("=")) {
            return "ERROR: Invalid UPDATE syntax";
        }

        String tableName = tokens[1];
        Table table = tables.get(tableName);
        if (table == null) {
            return "ERROR: Table not found";
        }

        String setColumn = tokens[3];
        String newValue = tokens[5];

        if (!table.getColumns().contains(setColumn)) {
            return "ERROR: Column not found";
        }

        List<Map<String, String>> rows = table.getRows();
        int updatedCount = 0;

        // If there's a WHERE clause
        if (tokens.length > 6 && tokens[6].toUpperCase().equals("WHERE")) {
            List<WhereCondition> conditions = parseWhereClause(tokens, 7);
            
            // Use index for the first condition if possible
            WhereCondition firstCondition = conditions.get(0);
            if (firstCondition.operator.equals("=")) {
                // Get matching rows using index
                List<Map<String, String>> matchingRows = table.findRowsByColumnValue(
                    firstCondition.column, 
                    firstCondition.value
                );
                
                // Update matching rows that satisfy all conditions
                for (Map<String, String> row : matchingRows) {
                    if (evaluateConditions(conditions, row)) {
                        row.put(setColumn, newValue);
                        updatedCount++;
                    }
                }
            } else {
                // Full scan for non-equality conditions
                for (Map<String, String> row : rows) {
                    if (evaluateConditions(conditions, row)) {
                        row.put(setColumn, newValue);
                        updatedCount++;
                    }
                }
            }
        } else {
            // Update all rows if no WHERE clause
            for (Map<String, String> row : rows) {
                row.put(setColumn, newValue);
                updatedCount++;
            }
        }

        return String.format("Table %s updated. %d rows affected.", tableName, updatedCount);
    }

    public String create(String[] tokens) {
        if (!tokens[1].equalsIgnoreCase("TABLE")) {
            return "ERROR: Invalid CREATE TABLE syntax";
        }

        String tableName = tokens[2];
        Table existingTable = tables.get(tableName);
        if (existingTable != null) {
            return "ERROR: Table already exists";
        }

        String columnList = queryBetweenParentheses(tokens, 3);
        List<String> columns = parseValuesToList(columnList);

        Table newTable = new Table(tableName, columns);
        tables.put(tableName, newTable);

        return "Table " + tableName + " created";
    }

    /*
     * HELPER METHODS
     */

    private Map<String, String> createRowMap(List<String> columns, List<String> values) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }
        return row;
    }

    private boolean evaluateConditions(List<WhereCondition> conditions, Map<String, String> row) {
        return conditions.stream().allMatch(condition -> condition.evaluate(row));
    }

    private String queryBetweenParentheses(String[] tokens, int startIndex) {
        StringBuilder result = new StringBuilder();
        for (int i = startIndex; i < tokens.length; i++) {
            result.append(tokens[i]).append(" ");
        }
        return result.toString().trim().replaceAll("\\(", "").replaceAll("\\)", "");
    }

    private List<String> parseValuesToList(String valueList) {
        return Arrays.stream(valueList.split(","))
                     .map(String::trim)
                     .toList();
    }

    private List<WhereCondition> parseWhereClause(String[] tokens, int startIndex) {
        List<WhereCondition> conditions = new ArrayList<>();

        for (int i = startIndex; i < tokens.length; i += 4) {
            WhereCondition condition = new WhereCondition(
                tokens[i], // column
                tokens[i + 1], // operator
                tokens[i + 2] // value
            );
            // [AND/OR] <- index i + 3
            if (hasLogicalOperator(tokens, i + 3)) {
                condition.logicalOperator = tokens[i + 3].toUpperCase();
            }

            conditions.add(condition);
        }

        return conditions;
    }

    private boolean hasLogicalOperator(String[] tokens, int index) {
        return index < tokens.length &&
                (tokens[index].equalsIgnoreCase("AND") ||
                        tokens[index].equalsIgnoreCase("OR"));
    }
}
