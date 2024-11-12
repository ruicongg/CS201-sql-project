package edu.smu.smusql.interfaces;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.smu.smusql.parser.WhereCondition;

public class RowEntry {
    private final Map<String, String> values;
    private boolean isDeleted;

    public RowEntry() {
        this.values = new HashMap<>();
        this.isDeleted = false;
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

    public String toString() {
        String str = "[";
        for (String key: values.keySet()) {
            str += key + ": " + values.get(key) + ", ";
        }
        if (str.length() > 1) {
            return str.substring(0, str.length() - 2) + "]";
        }

        return "[]";

    }

    public void setDeleted() {
        this.isDeleted = true;
    }

    public boolean isDeleted() {
        return this.isDeleted;
    }

    public boolean evaluateAllConditions(List<WhereCondition> conditions) {
        if (conditions.size() == 2 && conditions.get(0).getLogicalOperator().equals("OR")) {
            return conditions.stream().anyMatch(condition -> condition.evaluate(this));
        }
        return conditions.stream().allMatch(condition -> condition.evaluate(this));
    }
}
