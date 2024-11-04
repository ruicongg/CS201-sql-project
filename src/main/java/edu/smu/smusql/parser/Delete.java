package edu.smu.smusql.parser;

import java.util.List;

/*
 * Object used for greater clarity of
 * required parameters for SQL statement
 */
public class Delete {
    
    private String tablename;
    private List<WhereCondition> conditions;

    public Delete(String tablename, List<WhereCondition> conditions) {
        this.tablename = tablename;
        this.conditions = conditions;
    }
    
    public String getTablename() {
        return tablename;
    }
    public List<WhereCondition> getConditions() {
        return conditions;
    }
}
