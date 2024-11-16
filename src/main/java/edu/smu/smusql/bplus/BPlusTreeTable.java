package edu.smu.smusql.bplus;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.smu.smusql.interfaces.RowEntry;

public class BPlusTreeTable {
    private List<String> columns;
    private Map<String, BPlusTree> columnTrees;
    private List<RowEntry> rows;
    private int numRows;

    public BPlusTreeTable(List<String> columns) {
        this.columns = columns;
        this.columnTrees = new HashMap<>();
        this.rows = new ArrayList<>();
        // Initialize B+ trees for all columns
        for (String column : columns) {
            columnTrees.put(column, new BPlusTree());
        }
    }

    public List<String> getColumns() {
        return columns;
    }

    public BPlusTree getTreeForColumn(String column) {
        return columnTrees.get(column);
    }

    public void addRow(RowEntry row) {

        for (String column : columns) {
            String value = row.getValue(column);
            columnTrees.get(column).insert(value, numRows);
        }
        rows.add(row);
        numRows++;
    }

    public List<RowEntry> getAllEntries() {
        List<RowEntry> result = new ArrayList<>();
        for (RowEntry row : rows) {
            if (!row.isDeleted()) {
                result.add(row);
            }
        }
        return result;
    }

    public RowEntry getRow(int index) {
        return rows.get(index);
    }

    public void updateRow(int index, String column, String value) {
        rows.get(index).addOrUpdateValue(column, value);
    }

}
