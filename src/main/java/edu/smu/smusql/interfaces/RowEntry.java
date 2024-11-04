package edu.smu.smusql.interfaces;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.smu.smusql.parser.WhereCondition;

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

    public void addOrUpdateValue(String column, String value) {
        values.put(column, value);
    }

    public boolean evaluateAllConditions(List<WhereCondition> conditions) {
        return conditions.stream().allMatch(condition -> condition.evaluate(this));
    }
}
