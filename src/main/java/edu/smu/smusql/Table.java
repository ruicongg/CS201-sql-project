package edu.smu.smusql;

import java.util.*;

public class Table {
    private String name;
    private List<String> columns;
    private List<Map<String, String>> rows;
    // v1: improves on sample by using indices to speed up findRowsByColumnValue
    // maps column name to (map of value to list of row indices)
    private Map<String, Map<String, List<Integer>>> indices;

    public Table(String name, List<String> columns) {
        this.name = name;
        this.columns = columns;
        this.rows = new ArrayList<>();
        this.indices = new HashMap<>();
        // Initialize index for each column
        for (String column : columns) {
            indices.put(column, new HashMap<>());
        }
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Map<String, String>> getRows() {
        return rows;
    }

    public void addRow(Map<String, String> row) {
        rows.add(row);
        int rowIndex = rows.size() - 1;

        // for each column in the row, update the index
        for (Map.Entry<String, String> entry : row.entrySet()) {
            String column = entry.getKey();
            Map<String, List<Integer>> columnIndex = indices.get(column);
            columnIndex.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(rowIndex);
        }
    }

    

    public List<Map<String, String>> findRowsByColumnValue(String columnName, String value) {
        List<Map<String, String>> result = new ArrayList<>();
        Map<String, List<Integer>> columnIndex = indices.get(columnName);

        if (columnIndex != null && columnIndex.containsKey(value)) {
            for (Integer rowIndex : columnIndex.get(value)) {
                result.add(rows.get(rowIndex));
            }
        }
        return result;
    }

    public void setRows(List<Map<String, String>> newRows) {
        this.rows = newRows;
        // Rebuild indices after setting new rows
        rebuildIndices();
    }

    private void rebuildIndices() {
        // Clear all existing indices
        for (String column : columns) {
            indices.put(column, new HashMap<>());
        }
        
        // Rebuild indices for all rows
        for (int i = 0; i < rows.size(); i++) {
            Map<String, String> row = rows.get(i);
            for (Map.Entry<String, String> entry : row.entrySet()) {
                String column = entry.getKey();
                String value = entry.getValue();
                indices.get(column)
                      .computeIfAbsent(value, k -> new ArrayList<>())
                      .add(i);
            }
        }
    }
}
