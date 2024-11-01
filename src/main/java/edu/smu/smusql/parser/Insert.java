package edu.smu.smusql.parser;

import java.util.List;

/*
 * Object used for greater clarity of
 * required parameters for SQL statement
 */
public class Insert {
    private String tablename;
    private List<String> values;
    
    public Insert(String tablename, List<String> values) {
        this.tablename = tablename;
        this.values = values;
    }

    public String getTablename() {
        return tablename;
    }

    public List<String> getValues() {
        return values;
    }
}
