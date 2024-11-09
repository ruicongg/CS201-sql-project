package edu.smu.smusql.parser;

import java.util.List;

/*
 * Object used for greater clarity of
 * required parameters for SQL statement
 */
public class Update {
    private String tablename;
    private String columnname;
    private String value;
    private List<WhereCondition> conditions;

    public Update(String tablename, String columnname, String value, List<WhereCondition> conditions) {
        this.tablename = tablename;
        this.columnname = columnname;
        this.value = value;
        this.conditions = conditions;
    }

    public String getTablename() {
        return tablename;
    }

    public String getColumnname() {
        return columnname;
    }

    public String getValue() {
        return value;
    }

    public List<WhereCondition> getConditions() {
        return conditions;
    }
}
