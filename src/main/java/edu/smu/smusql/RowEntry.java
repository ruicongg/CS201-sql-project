package edu.smu.smusql;

import java.util.HashMap;
import java.util.Map;

public class RowEntry {
    private final Map<String, String> values;

    public RowEntry() {
        this.values = new HashMap<>();
    }

    public RowEntry(Map<String, String> values) {
        this.values = new HashMap<>(values);
    }

    public String getValue(String column) {
        return values.getOrDefault(column, "NULL");
    }

    public void addValue(String column, String value) {
        values.put(column, value);
    }
}
