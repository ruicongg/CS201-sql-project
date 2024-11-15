package edu.smu.smusql.bplus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.smu.smusql.interfaces.RowEntry;

public class BPlusTreeTable {
    private List<String> columns;
    private Map<String, BPlusTree> columnTrees;

    public BPlusTreeTable(List<String> columns) {
        this.columns = columns;
        this.columnTrees = new HashMap<>();
        
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
            columnTrees.get(column).insert(value, row);
        }
    }

    public List<RowEntry> getAllEntries() {
        if (columns.size() == 0) {
            return new ArrayList<RowEntry>();
        }
        return getTreeForColumn(columns.get(0)).searchAll();
    }

}
