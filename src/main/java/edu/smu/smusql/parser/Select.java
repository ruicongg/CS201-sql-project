package edu.smu.smusql.parser;

import java.util.List;
import edu.smu.smusql.WhereCondition;

/*
 * Object used for greater clarity of
 * required parameters for SQL statement
 */
public class Select {
    private String tablename;
    private List<WhereCondition> conditions;
    
    public Select(String tablename, List<WhereCondition> conditions) {
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
