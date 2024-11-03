package edu.smu.smusql;

import java.util.*;

public class Table {
    private String name;
    private List<String> columns;
    private List<RowEntry> rows;
    private final Map<String, Map<String, List<RowEntry>>> indices;  // column -> value -> rows

    public Table(String name, List<String> columns) {
        this.name = name;
        this.columns = columns;
        this.rows = new ArrayList<>();
        this.indices = new HashMap<>();
        
        // Initialize indices for all columns
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

    public List<RowEntry> getRows() {
        return rows;
    }

    public void addRow(RowEntry row) {
        rows.add(row);

        // Update indices
        for (String column : columns) {
            String value = row.getValue(column);
            indices.get(column)
                  .computeIfAbsent(value, k -> new ArrayList<>())
                  .add(row);
        }
    }

    public List<RowEntry> findRowsByColumnValue(String columnName, String value) {
        Map<String, List<RowEntry>> columnIndex = indices.get(columnName);
        if (columnIndex == null) {
            return Collections.emptyList();
        }
        return columnIndex.getOrDefault(value, Collections.emptyList());
    }

    public void setRows(List<RowEntry> newRows) {
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
            RowEntry row = rows.get(i);
            for (String column : columns) {
                String value = row.getValue(column);
                indices.get(column)
                      .computeIfAbsent(value, k -> new ArrayList<>())
                      .add(row);
            }
        }
    }
}
