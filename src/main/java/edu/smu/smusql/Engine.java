package edu.smu.smusql;

import java.util.*;
import java.util.stream.Collectors;

import edu.smu.smusql.parser.Parser;
import edu.smu.smusql.parser.InvalidCommandException;
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
        Table table = tables.get(tableName);
        if (table == null) {
            throw new InvalidCommandException("ERROR: Table not found");
        }

        List<String> values = insert.getValues();
        List<String> columns = table.getColumns();

        if (values.size() != columns.size()) {
            throw new InvalidCommandException("ERROR: Column count doesn't match value count");
        }

        Map<String, String> row = createRowMap(columns, values);
        table.addRow(row);
        return "Row inserted into " + tableName;
    }

    public String delete(Delete delete) {
        
        String tableName = delete.getTablename();
        Table table = tables.get(tableName);
        if (table == null) {
            throw new InvalidCommandException("ERROR: Table not found");
        }
        List<WhereCondition> conditions = delete.getConditions();
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

    public String select(Select select) {
    

        String tableName = select.getTablename();
        Table table = tables.get(tableName);
        if (table == null) {
            throw new InvalidCommandException("ERROR: Table not found");
        }

        List<String> columns = table.getColumns();
        List<Map<String, String>> rows;

        // If there's a WHERE clause
        if (select.getConditions().size() > 0) {
            List<WhereCondition> conditions = select.getConditions();
            
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

    public String update(Update update) {


        String tableName = update.getTablename();
        Table table = tables.get(tableName);
        if (table == null) {
            throw new InvalidCommandException("ERROR: Table not found");
        }

        String setColumn = update.getColumnname();
        String newValue = update.getValue();

        if (!table.getColumns().contains(setColumn)) {
            throw new InvalidCommandException("ERROR: Column not found");
        }

        List<Map<String, String>> rows = table.getRows();
        int updatedCount = 0;

        // If there's a WHERE clause
        if (update.getConditions().size() > 0) {
            List<WhereCondition> conditions = update.getConditions();
            
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

}
